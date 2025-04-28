#define DUCKDB_EXTENSION_MAIN

#include "example_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

//======================================================================================================================
// Extension Implementation
//======================================================================================================================

namespace duckdb {

namespace {

//----------------------------------------------------------------------------------------------------------------------
// Example Aggregate (Simple State)
//----------------------------------------------------------------------------------------------------------------------
// This is a pretty simple example of how to implement an aggregate function in DuckDB.
// It is a sum aggregate function that takes a single double input and returns the sum of all inputs, as a double.
// Most of the complexity stems from the fact that DuckDB is a vectorized database, so we need to process multiple
// inputs, and potentially multiple states at once.
// To make it easier to follow, I've grouped all the required callbacks as static methods in a single struct.
struct ExampleSumAggregateFunction {

	// When defining an aggregate function, we need to define the "state" that we will fold each row into.
	// In this case, our state is simple, is just a double that will hold the sum of all input values.
	// Its fixed-size, trivially destructible and copyable, so we can let DuckDB handle the memory management.
	struct State {
		double total_sum = 0.0;
	};

	// The aggregate function needs to know how big the state is so it can allocate enougn memory
	static idx_t StateSize(const AggregateFunction &) {
		return sizeof(State);
	}

	// This callback is used to "initialize" the state. It is called when the aggregate function is first created.
	// In this case, our state is trivialy constructible, so we could just memset to 0, but its better to use placement
	// new to avoid  potential issues with aliasing, undefined behavior or uninitialized memory.
	static void Initialize(const AggregateFunction &, data_ptr_t state_mem) {
		// Construct a new state into the memory alrady reserved by the aggregate
		new (state_mem) State();
	}


	// Now we need to define the function that will process the input values and update the state(s).
	static void Update(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vec,
					   idx_t count) {

		// Our aggregate only has one input, so disregard the "input_count"
		auto &input_vec = inputs[0];

		UnifiedVectorFormat input_format;
		input_vec.ToUnifiedFormat(count, input_format);

		UnifiedVectorFormat state_format;
		state_vec.ToUnifiedFormat(count, state_format);

		const auto input_ptr = UnifiedVectorFormat::GetData<double>(input_format);
		const auto state_ptr = UnifiedVectorFormat::GetData<State *>(state_format);

		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			const auto state_idx = state_format.sel->get_index(raw_idx);
			const auto input_idx = input_format.sel->get_index(raw_idx);

			if (!input_format.validity.RowIsValid(input_idx)) {
				// If the input row is null, we skip it
				continue;
			}

			// Otherwise, we add the imput value to the state
			auto &state = *state_ptr[state_idx];
			state.total_sum += input_ptr[input_idx];
		}
	}

	// For non-grouped aggregations, where there is only on state per thread, this callback will be invoked instead.
	// This is just an optional optimization however, if not provided (and set to nullptr) the regular "Update"
	// callback will be called in the case of non-grouped aggregation as well, the state vector will just be a
	// dictionary vector pointing to the same state for all rows.
	static void SimpleUpdate(Vector inputs[], AggregateInputData &, idx_t input_count, data_ptr_t state_ptr, idx_t count) {

		// Our aggregate only has one input, so disregard the "input_count"
		auto &input_vec = inputs[0];

		UnifiedVectorFormat input_format;
		input_vec.ToUnifiedFormat(count, input_format);

		const auto input_ptr = UnifiedVectorFormat::GetData<double>(input_format);

		// In this callback, there is only as single state, which we can retrieve from the state_ptr
		auto &state = *reinterpret_cast<State *>(state_ptr);

		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			const auto input_idx = input_format.sel->get_index(raw_idx);

			if (!input_format.validity.RowIsValid(input_idx)) {
				// If the input row is null, we skip it
				continue;
			}

			// Otherwise, we add the imput value to the state
			state.total_sum += input_ptr[input_idx];
		}
	}

	// This function is used to combine states across threads once all input has been processed.
	static void Combine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {

		UnifiedVectorFormat source_format;
		source_vec.ToUnifiedFormat(count, source_format);

		const auto source_ptr = UnifiedVectorFormat::GetData<State *>(source_format);
		const auto target_ptr = FlatVector::GetData<State *>(target_vec);

		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			const auto state_idx = source_format.sel->get_index(raw_idx);

			auto &source_state = *source_ptr[state_idx];
			auto &target_state = *target_ptr[raw_idx];

			// Combine the states, in this case, by adding the total sum
			target_state.total_sum += source_state.total_sum;
		}
	}

	// Once all input has been processed, and the states in each thread has been combined, we get to the final step
	// where we extract the resulting values from the states and write them to the result vector.
	static void Finalize(Vector &state_vec, AggregateInputData &, Vector &result, idx_t count,
	                     idx_t offset) {

		UnifiedVectorFormat state_format;
		state_vec.ToUnifiedFormat(count, state_format);

		const auto state_ptr = UnifiedVectorFormat::GetData<State *>(state_format);
		const auto result_ptr = FlatVector::GetData<double>(result);

		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			auto &state = *state_ptr[state_format.sel->get_index(raw_idx)];
			const auto out_idx = raw_idx + offset;

			// Finalize the state, in this case, just copy the total sum to the result
			result_ptr[out_idx] = state.total_sum;
		}
	}

	static void Register(DatabaseInstance &db) {
		// Provide all our callbacks
		AggregateFunction example_sum({LogicalType::DOUBLE}, LogicalType::DOUBLE,
		              StateSize, Initialize, Update, Combine, Finalize, SimpleUpdate);
		example_sum.name = "example_sum";

		// Register the function
		ExtensionUtil::RegisterFunction(db, example_sum);
	}
};

//----------------------------------------------------------------------------------------------------------------------
// Example Aggregate (Complex State)
//----------------------------------------------------------------------------------------------------------------------
// This is a more complex example, that showcases how to implement an aggregate function with a dynamically allocated
// state, which needs to be freed, and which can optionally be destructively combined.
// It is a function that simply formats all input doubles into a string, separated by a comma.
//
// Note that DuckDB does not guarantee that the order of the input rows is preserved when aggregating. If that is
// important, you should use the `ORDER BY` within the GROUP BY clause, e.g. `GROUP BY (id ORDER BY id DESC)`.
//
struct ExampleConcatenationAggregateFunction {

	// The state is a list of all input rows in the order they were processed within the group.
	struct State {
		vector<double> values;
	};

	// Initializse the state
	static void Initialize(const AggregateFunction &, data_ptr_t state_mem) {
		// Construct a new state into the memory alrady reserved by the aggregate
		// In this case, its actually important that we call the constructor so that the vector is initialized
		new (state_mem) State();
	}

	// Get the size of the state
	// Note that this is only the fixed-size portion of the state, the vector itself is dynamically allocated on
	// the heap. Unfortunately this means that DuckDB wont be able to spill the whole state to disk as it is not  aware
	// of that memory, and we also need to provide our own destructor callback as well to make sure its destroyed
	// properly.
	// In the aggregate callbacks, you do have access to a ArenaAllocator (through the "AggregateInputData") which can
	// be used to allocate memory which DuckDB will automatically track and free when the aggregate is destroyed.
	// This is should thus be preferred over using e.g. std::vector or std::unique_ptr to hold dynamic memory in the
	// state.
	static idx_t StateSize(const AggregateFunction &) {
		return sizeof(State);
	}

	// Destroy the states when the aggregate is over.
	static void Destroy(Vector &state_vec, AggregateInputData &aggr, idx_t count) {
		UnifiedVectorFormat state_format;
		state_vec.ToUnifiedFormat(count, state_format);

		const auto state_ptr = UnifiedVectorFormat::GetData<State *>(state_format);
		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			const auto row_idx = state_format.sel->get_index(raw_idx);
			if (state_format.validity.RowIsValid(row_idx)) {
				auto &state = *state_ptr[row_idx];

				// Call the destructor of the state to free any dynamically allocated memory
				state.~State();
			}
		}
	}

	// Update the state with the input values
	static void Update(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vec,
				   idx_t count) {

		// Our aggregate only has one input, so disregard the "input_count"
		auto &input_vec = inputs[0];

		UnifiedVectorFormat input_format;
		input_vec.ToUnifiedFormat(count, input_format);

		UnifiedVectorFormat state_format;
		state_vec.ToUnifiedFormat(count, state_format);

		const auto input_ptr = UnifiedVectorFormat::GetData<double>(input_format);
		const auto state_ptr = UnifiedVectorFormat::GetData<State *>(state_format);

		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			const auto state_idx = state_format.sel->get_index(raw_idx);
			const auto input_idx = input_format.sel->get_index(raw_idx);

			if (!input_format.validity.RowIsValid(input_idx)) {
				// If the input row is null, we skip it
				continue;
			}

			// Otherwise, we add the imput value to the state
			auto &state = *state_ptr[state_idx];
			state.values.push_back(input_ptr[input_idx]);
		}
	}

	// In some cases, DuckDB knows that the old state is not needed anymore, and in those cases it might be beneficial
	// to "destructively" combine the states, i.e. move the values from the old state to the new state,
	// instead of copying them. Or just clearing the old state to free up some memory.
	// However, this is also just an optimization, but can be useful if the state is large or complex.
	static void Absorb(Vector &state_vec, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
		D_ASSERT(aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE);

		UnifiedVectorFormat state_format;
		state_vec.ToUnifiedFormat(count, state_format);

		const auto state_ptr = UnifiedVectorFormat::GetData<State *>(state_format);
		const auto combined_ptr = FlatVector::GetData<State *>(combined);

		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			const auto state_idx = state_format.sel->get_index(raw_idx);
			auto &state = *state_ptr[state_idx];

			// Steal the entries from the state, and clear the old state
			// Or move all values, or whatever is more efficient in your case.
			// I know, in this example it doesnt really matter cause copying doubles is cheap.
			// But you can imagine that the values are strings or something that has resource ownership

			auto &combined_state = *combined_ptr[raw_idx];

			combined_state.values.reserve(combined_state.values.size() + state.values.size());
			for(auto & value : state.values) {
				combined_state.values.push_back(std::move(value));
			}
			state.values.clear();
		}
	}

	// Merge the states. We either combine by copying, or moving ("absorbing") when possible.
	static void Combine(Vector &state_vec, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {

		//Can we safely use destructive combining?
		if (aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
			// Call our absorb implementation instead!
			Absorb(state_vec, combined, aggr_input_data, count);
			return;
		}

		UnifiedVectorFormat state_format;
		state_vec.ToUnifiedFormat(count, state_format);

		const auto state_ptr = UnifiedVectorFormat::GetData<State *>(state_format);
		const auto combined_ptr = FlatVector::GetData<State *>(combined);

		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			const auto state_idx = state_format.sel->get_index(raw_idx);
			const auto &state = *state_ptr[state_idx];

			// We can't steal the list, we need to clone and append all elements
			auto &combined_state = *combined_ptr[raw_idx];

			// Just copy the values from the state to the combined state
			for (auto &value : state.values) {
				combined_state.values.push_back(value);
			}
		}
	}

	// Now convert the state into a string and write it to the result vector.
	static void Finalize(Vector &state_vec, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                     idx_t offset) {

		UnifiedVectorFormat state_format;
		state_vec.ToUnifiedFormat(count, state_format);

		const auto state_ptr = UnifiedVectorFormat::GetData<State *>(state_format);

		auto &mask = FlatVector::Validity(result);
		const auto result_ptr = FlatVector::GetData<string_t>(result);

		for (idx_t raw_idx = 0; raw_idx < count; raw_idx++) {
			auto &state = *state_ptr[state_format.sel->get_index(raw_idx)];
			const auto out_idx = raw_idx + offset;

			if (state.values.empty()) {
				// If the state is empty, we set the result to null
				mask.SetInvalid(out_idx);
			} else {
				// Otherwise, we format the values into a string and write it to the result
				string result_str;
				for (const auto &value : state.values) {
					if (!result_str.empty()) {
						result_str += ",";
					}
					result_str += std::to_string(value);
				}

				// Assign and attach the string to the result vector
				result_ptr[out_idx] = StringVector::AddString(result, result_str);
			}
		}
	}

	static void Register(DatabaseInstance &db) {
		// Provide all our callbacks
		AggregateFunction example_concat({LogicalType::DOUBLE}, LogicalType::VARCHAR,
		              StateSize, Initialize, Update, Combine, Finalize, nullptr);

		example_concat.name = "example_concat";

		// Set the destructor callback to free the state
		example_concat.destructor = Destroy;

		// Register the function
		ExtensionUtil::RegisterFunction(db, example_concat);
	}
};

//----------------------------------------------------------------------------------------------------------------------
// Example aggregate (using templates)
//----------------------------------------------------------------------------------------------------------------------
// DuckDB also provides a bunch of templates to make it "easier" to implement aggregate functions by just defining
// the row-by-row operations, and leaving the dictionary/vectorization to DuckDB.
// However, this is not always the best approach depending on how complex your state is, and IMO the templates
// dont always make it easier to understand what is going on.
struct ExampleTemplatedSumAggregateFunction {

	struct State {
		double total_sum = 0.0;
	};

	template <class STATE>
	static void Initialize(STATE &state) {
		state.total_sum = 0.0;
	}

	// Called for each state pair to combine
	template <class STATE = State, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.total_sum += source.total_sum;
	}

	// Called for each input row
	template <class INPUT_TYPE = double, class STATE = State, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &aggregate) {
		state.total_sum += input;
	}

	// As an optimization, we can provide a callback that will be invoked when the input is constant.
	// E.g. we get the same value for all rows in the group.
	// For our sum aggregate, we can thus just multiply the input value by the number of rows in the group.
	template <class INPUT_TYPE = double, class STATE = State, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &agg, idx_t count) {
		state.total_sum += (input * count);
	}

	template <class RESULT_TYPE = double, class STATE = State>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		target = state.total_sum;
	}

	static bool IgnoreNull() {
		return true;
	}

	static void Register(DatabaseInstance &db) {

		auto func = AggregateFunction::UnaryAggregate<State, double, double, ExampleTemplatedSumAggregateFunction>(
			LogicalType::DOUBLE, LogicalType::DOUBLE);
		func.name = "example_templated_sum";

		ExtensionUtil::RegisterFunction(db, func);
	}
};


} // namespace

} // namespace duckdb

//======================================================================================================================
// Extension Registration
//======================================================================================================================

namespace duckdb {

void ExampleExtension::Load(DuckDB &db) {
	auto &instance = *db.instance;

	// Register the example aggregate function
	ExampleSumAggregateFunction::Register(instance);

	// Register the example concatenation aggregate function
	ExampleConcatenationAggregateFunction::Register(instance);

	// Register the example templated aggregate function
	ExampleTemplatedSumAggregateFunction::Register(instance);

}
std::string ExampleExtension::Name() {
	return "example";
}

std::string ExampleExtension::Version() const {
#ifdef EXT_VERSION_EXAMPLE
	return EXT_VERSION_EXAMPLE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void example_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ExampleExtension>();
}

DUCKDB_EXTENSION_API const char *example_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
