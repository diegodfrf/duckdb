#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#endif

#include <iostream>
using namespace duckdb;

// in this example we build a simple volcano model executor on top of the DuckDB logical plans
// for simplicity, the executor only handles integer values and doesn't handle null values

void ExecuteQuery(Connection &con, const string &query);
void CreateFunction(Connection &con, string name, vector<LogicalType> arguments, LogicalType return_type);
void CreateAggregateFunction(Connection &con, string name, vector<LogicalType> arguments, LogicalType return_type);

//===--------------------------------------------------------------------===//
// Example Using DuckDB Catalog/Tables
//===--------------------------------------------------------------------===//
void RunExampleDuckDBCatalog() {
	// in this example we use the DuckDB CREATE TABLE syntax to create tables to link against
	// this works and the tables are easy to define, but since the tables are empty there are no statistics available
	// we can use our own table functions (see RunExampleTableScan), but this is slightly more involved

	DBConfig config;
	config.initialize_default_database = false;

	// disable the statistics propagator optimizer
	// this is required since the statistics propagator will truncate our plan
	// (e.g. it will recognize the table is empty that satisfy the predicate i=3
	//       and then prune the entire plan)
	config.disabled_optimizers.insert(OptimizerType::STATISTICS_PROPAGATION);
	// we don't support filter pushdown yet in our toy example
	config.disabled_optimizers.insert(OptimizerType::FILTER_PUSHDOWN);

	DuckDB db(nullptr, &config);
	Connection con(db);

	// we perform an explicit BEGIN TRANSACTION here
	// since "CreateFunction" will directly poke around in the catalog
	// which requires an active transaction
	con.Query("BEGIN TRANSACTION");

	// register dummy tables (for our binding purposes)
	con.Query("CREATE TABLE mytable(i INTEGER, j INTEGER)");
	con.Query("CREATE TABLE myothertable(k INTEGER)");
	// contents of the tables
	// mytable:
	// i: 1, 2, 3, 4, 5
	// j: 2, 3, 4, 5, 6
	// myothertable
	// k: 1, 10, 20
	// (see MyScanNode)

	// register functions and aggregates (for our binding purposes)
	CreateFunction(con, "+", {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER);
	CreateAggregateFunction(con, "count_star", {}, LogicalType::BIGINT);
	CreateAggregateFunction(con, "sum", {LogicalType::INTEGER}, LogicalType::INTEGER);

	con.Query("COMMIT");

	// standard projections
	ExecuteQuery(con, "SELECT * FROM mytable");
	ExecuteQuery(con, "SELECT i FROM mytable");
	ExecuteQuery(con, "SELECT j FROM mytable");
	ExecuteQuery(con, "SELECT k FROM myothertable");
	// some simple filter + projection
	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE i=3 OR i=4");
	// more complex filters
	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE (i<=2 AND j<=3) OR (i=4 AND j=5)");
	// aggregate
	ExecuteQuery(con, "SELECT COUNT(*), SUM(i) + 1, SUM(j) + 2 FROM mytable WHERE i>2");
	// with a subquery
	ExecuteQuery(con,
	             "SELECT a, b + 1, c + 2 FROM (SELECT COUNT(*), SUM(i), SUM(j) FROM mytable WHERE i > 2) tbl(a, b, c)");
}

//===--------------------------------------------------------------------===//
// Example Using Custom Function over DuckDB Catalog
//===--------------------------------------------------------------------===//
void RunCustomFunctionDuckDB() {
	// in this example we use the DuckDB CREATE TABLE syntax to create tables to link against
	// this works and the tables are easy to define, but since the tables are empty there are no statistics available
	// we can use our own table functions (see RunExampleTableScan), but this is slightly more involved

	DBConfig config;
	config.initialize_default_database = true;

	// disable the statistics propagator optimizer
	// this is required since the statistics propagator will truncate our plan
	// (e.g. it will recognize the table is empty that satisfy the predicate i=3
	//       and then prune the entire plan)
	config.disabled_optimizers.insert(OptimizerType::STATISTICS_PROPAGATION);
	// we don't support filter pushdown yet in our toy example
	config.disabled_optimizers.insert(OptimizerType::FILTER_PUSHDOWN);

	DuckDB db(nullptr, &config);
	Connection con(db);

	// we perform an explicit BEGIN TRANSACTION here
	// since "CreateFunction" will directly poke around in the catalog
	// which requires an active transaction
	con.Query("BEGIN TRANSACTION");

	// register dummy tables (for our binding purposes)
	con.Query("CREATE TABLE mytable(i INTEGER, j INTEGER)");
	con.Query("CREATE TABLE myothertable(k INTEGER)");
	con.Query(R"(create table lineitem ( l_orderkey    integer ,
                             l_partkey     integer ,
                             l_suppkey     integer ,
                             l_linenumber  integer ,
                             l_quantity    decimal(15,2) ,
                             l_extendedprice  decimal(15,2) ,
                             l_discount    decimal(15,2) ,
                             l_tax         decimal(15,2) ,
                             l_returnflag  char(1) ,
                             l_linestatus  char(1) ,
                             l_shipdate    date ,
                             l_commitdate  date ,
                             l_receiptdate date ,
                             l_shipinstruct char(25) ,
                             l_shipmode     char(10) ,
						     l_comment      varchar(44) ))");
	// contents of the tables
	// mytable:
	// i: 1, 2, 3, 4, 5
	// j: 2, 3, 4, 5, 6
	// myothertable
	// k: 1, 10, 20
	// (see MyScanNode)

	// register functions and aggregates (for our binding purposes)
//	CreateFunction(con, "*", {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER);
//	CreateFunction(con, "+", {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER);
//	CreateFunction(con, "-", {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER);
//	CreateFunction(con, "isnull", {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER);
//	CreateAggregateFunction(con, "count_star", {}, LogicalType::BIGINT);
//	CreateAggregateFunction(con, "sum", {LogicalType::INTEGER}, LogicalType::INTEGER);
//	CreateAggregateFunction(con, "avg", {LogicalType::INTEGER}, LogicalType::INTEGER);

	con.Query("COMMIT");

	// standard projections
	//	ExecuteQuery(con, "SELECT coalesce(i, 0) FROM mytable");//OK
	//	ExecuteQuery(con, "SELECT ifnull(i, 0) FROM mytable");//OK
	//	ExecuteQuery(con, "SELECT isnull(i, 0) FROM mytable");//ERROR
	//	ExecuteQuery(con, "SELECT * FROM mytable");
	//	ExecuteQuery(con, "SELECT i FROM mytable");
	//	ExecuteQuery(con, "SELECT j FROM mytable");
	//	ExecuteQuery(con, "SELECT k FROM myothertable");
	//	// some simple filter + projection
	//	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE i=3 OR i=4");
	//	ExecuteQuery(con, "SELECT * FROM mytable WHERE i>4 order by i desc, j desc");
	//	ExecuteQuery(con, "SELECT * FROM mytable t1 join mytable t2 on t1.i=t2.j");
	//	// more complex filters
	//	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE (i<=2 AND j<=3) OR (i=4 AND j=5)");
	//	// aggregate
	//	ExecuteQuery(con, "SELECT COUNT(*), SUM(i) + 1, SUM(j) + 2 FROM mytable WHERE i>2");
	//	// with a subquery
	//	ExecuteQuery(con,
	//	             "SELECT a, b + 1, c + 2 FROM (SELECT COUNT(*), SUM(i), SUM(j) FROM mytable WHERE i > 2) tbl(a, b, c)");
//		ExecuteQuery(con, R"(select
//	                l_returnflag,
//	                l_linestatus,
//	                sum(l_quantity) as sum_qty,
//	                sum(l_extendedprice) as sum_base_price,
//	                sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
//	                sum(l_extendedprice*(1-l_discount)*(1+l_tax))
//	                    as sum_charge,
//	                avg(l_quantity) as avg_qty,
//	                avg(l_extendedprice) as avg_price,
//	                avg(l_discount) as avg_disc,
//	                count(*) as count_order
//	            from
//	                lineitem
//	            where
//	                l_shipdate <= date '1998-12-01' - interval '90' day
//	            group by
//	                l_returnflag,
//	                l_linestatus
//	            order by
//	                l_returnflag,
//		l_linestatus)");

//
//	ExecuteQuery(con, R"(select
//	                l_returnflag,
//	                l_linestatus
//	            from
//	                lineitem
//				where l_linenumber=3 OR l_linenumber=4
//				order by l_linestatus desc
//				limit 5 offset 3)");
	ExecuteQuery(con, R"(select l_linenumber from lineitem where l_quantity > 4)");
//	ExecuteQuery(con, R"(SELECT l_linenumber FROM lineitem WHERE l_linenumber=3 OR l_linenumber=4)");
//	ExecuteQuery(con, R"(SELECT avg(l_linenumber) FROM lineitem)");
}
//===--------------------------------------------------------------------===//
// Example Using Custom Scan Function
//===--------------------------------------------------------------------===//
void CreateMyScanFunction(Connection &con);

unique_ptr<TableFunctionRef> MyReplacementScan(const string &table_name, void *data) {
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("my_scan", move(children));
	return table_function;
}

void RunExampleTableScan() {
	// in this example we use our own TableFunction instead of the built-in "seq_scan"
	// this allows us to emit our own statistics without needing to insert them into the DuckDB tables
	// it also allows us to define ourselves what we do/do not support
	// (e.g. we can disable projection or filter pushdown in the table function if desired)
	// this means we don't need to disable optimizers anymore

	DBConfig config;
	config.initialize_default_database = false;
	config.replacement_scans.push_back(ReplacementScan(MyReplacementScan));

	DuckDB db(nullptr, &config);
	Connection con(db);

	// we perform an explicit BEGIN TRANSACTION here
	// since "CreateFunction" will directly poke around in the catalog
	// which requires an active transaction
	con.Query("BEGIN TRANSACTION");

	// register functions and aggregates (for our binding purposes)
	CreateFunction(con, "+", {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER);
	CreateAggregateFunction(con, "count_star", {}, LogicalType::BIGINT);
	CreateAggregateFunction(con, "sum", {LogicalType::INTEGER}, LogicalType::INTEGER);

	CreateMyScanFunction(con);

	con.Query("COMMIT");

	// standard projections
	ExecuteQuery(con, "SELECT * FROM mytable");
	ExecuteQuery(con, "SELECT i FROM mytable");
	ExecuteQuery(con, "SELECT j FROM mytable");
	ExecuteQuery(con, "SELECT k FROM myothertable");
	// some simple filter + projection
	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE i=3 OR i=4");
	// more complex filters
	ExecuteQuery(con, "SELECT i+1 FROM mytable WHERE (i<=2 AND j<=3) OR (i=4 AND j=5)");
	// aggregate
	ExecuteQuery(con, "SELECT COUNT(*), SUM(i) + 1, SUM(j) + 2 FROM mytable WHERE i>2");
	// with a subquery
	ExecuteQuery(con,
	             "SELECT a, b + 1, c + 2 FROM (SELECT COUNT(*), SUM(i), SUM(j) FROM mytable WHERE i > 2) tbl(a, b, c)");
}

//===--------------------------------------------------------------------===//
// Create Dummy Scalar/Aggregate Functions in the Catalog
//===--------------------------------------------------------------------===//
void CreateFunction(Connection &con, string name, vector<LogicalType> arguments, LogicalType return_type) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	// we can register multiple functions here if we want overloads
	// you may also want to set has_side_effects or varargs in the ScalarFunction (if required)
	ScalarFunctionSet set(name);
	set.AddFunction(ScalarFunction(move(arguments), move(return_type), nullptr));

	CreateScalarFunctionInfo info(move(set));
	catalog.CreateFunction(context, &info);
}

void CreateAggregateFunction(Connection &con, string name, vector<LogicalType> arguments, LogicalType return_type) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	// we can register multiple functions here if we want overloads
	AggregateFunctionSet set(name);
	set.AddFunction(AggregateFunction(move(arguments), move(return_type), nullptr, nullptr, nullptr, nullptr, nullptr));

	CreateAggregateFunctionInfo info(move(set));
	catalog.CreateFunction(context, &info);
}

//===--------------------------------------------------------------------===//
// Custom Table Scan Function
//===--------------------------------------------------------------------===//
struct MyBindData : public FunctionData {
	MyBindData(string name_p) : table_name(move(name_p)) {
	}

	string table_name;
};

// contents of the tables
// mytable:
// i: 1, 2, 3, 4, 5
// j: 2, 3, 4, 5, 6
// myothertable
// k: 1, 10, 20
// (see MyScanNode)
static unique_ptr<FunctionData> MyScanBind(ClientContext &context, vector<Value> &inputs,
                                           unordered_map<string, Value> &named_parameters,
                                           vector<LogicalType> &input_table_types, vector<string> &input_table_names,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto table_name = inputs[0].ToString();
	if (table_name == "mytable") {
		names.emplace_back("i");
		return_types.push_back(LogicalType::INTEGER);

		names.emplace_back("j");
		return_types.push_back(LogicalType::INTEGER);
	} else if (table_name == "myothertable") {
		names.emplace_back("k");
		return_types.push_back(LogicalType::INTEGER);
	} else {
		throw std::runtime_error("Unknown table " + table_name);
	}
	auto result = make_unique<MyBindData>(table_name);
	return move(result);
}

static unique_ptr<BaseStatistics> MyScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                   column_t column_id) {
	auto &bind_data = (MyBindData &)*bind_data_p;
	if (bind_data.table_name == "mytable") {
		if (column_id == 0) {
			// i: 1, 2, 3, 4, 5
			return make_unique<NumericStatistics>(LogicalType::INTEGER, Value::INTEGER(1), Value::INTEGER(5));
		} else if (column_id == 1) {
			// j: 2, 3, 4, 5, 6
			return make_unique<NumericStatistics>(LogicalType::INTEGER, Value::INTEGER(2), Value::INTEGER(6));
		}
	} else if (bind_data.table_name == "myothertable") {
		// k: 1, 10, 20
		return make_unique<NumericStatistics>(LogicalType::INTEGER, Value::INTEGER(1), Value::INTEGER(20));
	}
	return nullptr;
}

unique_ptr<NodeStatistics> MyScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (MyBindData &)*bind_data_p;
	if (bind_data.table_name == "mytable") {
		// 5 tuples
		return make_unique<NodeStatistics>(5, 5);
	} else if (bind_data.table_name == "myothertable") {
		return make_unique<NodeStatistics>(3, 3);
	}
	return nullptr;
}

void CreateMyScanFunction(Connection &con) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	TableFunction my_scan("my_scan", {LogicalType::VARCHAR}, nullptr, MyScanBind, nullptr, MyScanStatistics, nullptr,
	                      nullptr, MyScanCardinality);
	my_scan.projection_pushdown = true;
	my_scan.filter_pushdown = false;

	CreateTableFunctionInfo info(move(my_scan));
	catalog.CreateTableFunction(context, &info);
}

//===--------------------------------------------------------------------===//
// Example Execution Engine: Row-based volcano style that only supports int32
//===--------------------------------------------------------------------===//
class MyNode {
public:
	virtual ~MyNode() {
	}
	virtual vector<int> GetNextRow() = 0;

	unique_ptr<MyNode> child;
};

class MyPlanGenerator {
public:
	unique_ptr<MyNode> TransformPlan(LogicalOperator &op);
};

int32_t level = 0;

void printTabs(){
	std::cout << "[" << level << "]";
	for(int32_t i = 0; i < level; ++i)
	{
		std::cout << "  ";
	}
}

void Imprimir(unique_ptr<LogicalOperator> op){
	/*std::cout<<"Name: "<<op->GetName()<<"\n";
	std::cout<<"type: "<<(int)op->type<<"\n";*/
	/*std::cout<<"Params: "<<op->ParamsToString()<<"\n";

	for(auto& exp : op->expressions){
	    std::cout<<"exp: "<<exp->GetName()<<"\n";
	    std::cout<<"alias: "<<exp->alias<<"\n";
	}
	std::cout<<"\n";*/

	std::string dsl_calcite = "";

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	{
		auto logOp = unique_ptr<LogicalFilter>{static_cast<LogicalFilter*>(op.release())};
		if(!logOp) std::cout<<"Error at cast!\n";
		/*auto vec = logOp->GetColumnBindings();
		for(auto col : vec){
		    std::cout<<" - "<<col.table_index<<" - "<<col.column_index<<" ";
		}
		std::cout<<"\n";*/
		logOp->SplitPredicates();
		/*for(auto& exp : logOp->expressions){
		    std::cout<<"exp2: "<<exp->GetName()<<"\n";
		    std::cout<<"alias2: "<<exp->alias<<"\n";
		}*/
		op = unique_ptr<LogicalOperator>{static_cast<LogicalOperator*>(logOp.release())};
		dsl_calcite = "LogicalFilter(condition=[" + op->ParamsToString() + "])";
	}
	break;
	case LogicalOperatorType::LOGICAL_GET:
		dsl_calcite = "LogicalTableScan(table=[[" + op->ParamsToString() + "]])";
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	{
		dsl_calcite = "LogicalSort(";
		auto logOp = unique_ptr<LogicalOrder>{static_cast<LogicalOrder*>(op.release())};
		if(!logOp) std::cout<<"Error at cast!\n";

		for (idx_t i = 0; i < logOp->orders.size(); i++) {
			dsl_calcite += "sort" + std::to_string(i) + "=[" + logOp->orders[i].expression->GetName() + "]";
			dsl_calcite += ", ";
			std::string order_type = logOp->orders[i].type == OrderType::ASCENDING ? "ASC" : "DESC";
			dsl_calcite += "dir" + std::to_string(i) + "=[" + order_type + "]";

			if(i<logOp->orders.size()-1)
				dsl_calcite += ", ";
		}

		op = unique_ptr<LogicalOperator>{static_cast<LogicalOperator*>(logOp.release())};
		dsl_calcite += "])";
	}
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	{
		auto logOp = unique_ptr<LogicalComparisonJoin>{static_cast<LogicalComparisonJoin*>(op.release())};
		if(!logOp) std::cout<<"Error at cast!\n";

		dsl_calcite += "LogicalJoin(condition=[";

		for (auto &condition : logOp->conditions) {
			auto expr = make_unique<BoundComparisonExpression>(condition.comparison, condition.left->Copy(),
			                                                   condition.right->Copy());
			dsl_calcite += expr->ToString();
		}

		dsl_calcite += "], joinType=[";
		switch (logOp->join_type)
		{
		case JoinType::INNER:
			dsl_calcite += "inner";
			break;
		default:
			break;
		}
		dsl_calcite += "])";
		op = unique_ptr<LogicalOperator>{static_cast<LogicalOperator*>(logOp.release())};
	}
	break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
	{
		LogicalProjection* logOp = static_cast<LogicalProjection*>(op.get());

		string table;
		if(!op->children.empty() && op->children[0]->type == LogicalOperatorType::LOGICAL_GET)
		{
			table = op->children[0]->ParamsToString();
		}

		string params = logOp->ParamsToString();
		std::replace(params.begin(), params.end(), '\n', ',');
		dsl_calcite = "BindableTableScan(table=[[";
		dsl_calcite += table;
		dsl_calcite += "]], aliases=[[";
		dsl_calcite += params;
		dsl_calcite += "]])";
	}
	break;
	case LogicalOperatorType::LOGICAL_LIMIT:
	{
		LogicalLimit *logOp = static_cast<LogicalLimit *>(op.get());

		dsl_calcite = "LogicalLimit(";
		bool boffset = false;
		if (logOp->offset_val > 0) {
			boffset = true;
			dsl_calcite += "offset=[";
			dsl_calcite += std::to_string(logOp->offset_val);
			dsl_calcite += "]";
		}
		if (logOp->limit_val < std::numeric_limits<int64_t>::max()) {
			if(boffset) dsl_calcite += ", ";
			dsl_calcite += "fetch=[";
			dsl_calcite += std::to_string(logOp->limit_val);
			dsl_calcite += "]";
		}
		dsl_calcite += ")";
	}
	break;
	case LogicalOperatorType::LOGICAL_TOP_N:
	{
		struct top{
			string type;
			int32_t code;
			int32_t column_idx;
		};
		vector<top> tops;

		LogicalTopN *logOp = static_cast<LogicalTopN *>(op.get());

		std::string cardinality = std::to_string(logOp->estimated_cardinality);
		dsl_calcite = "LogicalSort(";

		int32_t count = 0;
		for(BoundOrderByNode& ord : logOp->orders){
			count++;
			top t;
			t.code = count - 1;
			if(ord.type == OrderType::ASCENDING) t.type = "ASC";
			if(ord.type == OrderType::DESCENDING) t.type = "DESC";
			if(ord.type == OrderType::ORDER_DEFAULT) t.type = "ASC";
			BoundReferenceExpression* be = static_cast<BoundReferenceExpression*>(ord.expression.get());
			t.column_idx = be->index;
			tops.push_back(t);
		}

		count = 0;

		string sorts;
		for(top& t: tops){
			sorts += ", sort" + std::to_string(t.code) + "=[$";
			sorts += std::to_string(t.column_idx);
			sorts += "]";
		}
		sorts.erase(0,2);
		dsl_calcite += sorts;

		for(top& t: tops){
			dsl_calcite += ", dir" + std::to_string(t.code) + "=[";
			dsl_calcite += t.type;
			dsl_calcite += "]";
		}

		bool boffset = false;
		if (logOp->offset > 0) {
			boffset = true;
			dsl_calcite += ", offset=[";
			dsl_calcite += std::to_string(logOp->offset);
			dsl_calcite += "]";
		}
		if (logOp->limit < std::numeric_limits<int64_t>::max()) {
			if(boffset) dsl_calcite += ", ";
			dsl_calcite += "fetch=[";
			dsl_calcite += std::to_string(logOp->limit);
			dsl_calcite += "]";
		}
		dsl_calcite += ")";
	}
	break;


	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	{
		dsl_calcite = "LOGICAL_AGGREGATE_AND_GROUP_BY";
	} break;

	case LogicalOperatorType::LOGICAL_INVALID:
	{
		dsl_calcite = "LOGICAL_INVALID";
	} break;
	case LogicalOperatorType::LOGICAL_WINDOW:
	{
		dsl_calcite = "LOGICAL_WINDOW";
	} break;
	case LogicalOperatorType::LOGICAL_UNNEST:
	{
		dsl_calcite = "LOGICAL_UNNEST";
	} break;

	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
	{
		dsl_calcite = "LOGICAL_COPY_TO_FILE";
	} break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
	{
		dsl_calcite = "LOGICAL_DISTINCT";
	} break;
	case LogicalOperatorType::LOGICAL_SAMPLE:
	{
		dsl_calcite = "LOGICAL_SAMPLE";
	} break;
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	{
		dsl_calcite = "LOGICAL_CHUNK_GET";
	} break;
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	{
		dsl_calcite = "LOGICAL_DELIM_GET";
	} break;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	{
		dsl_calcite = "LOGICAL_EXPRESSION_GET";
	} break;
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	{
		dsl_calcite = "LOGICAL_DUMMY_SCAN";
	} break;
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
	{
		dsl_calcite = "LOGICAL_EMPTY_RESULT";
	} break;
	case LogicalOperatorType::LOGICAL_CTE_REF:
	{
		dsl_calcite = "LOGICAL_CTE_REF";
	} break;
	case LogicalOperatorType::LOGICAL_JOIN:
	{
		dsl_calcite = "LOGICAL_JOIN";
	} break;
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	{
		dsl_calcite = "LOGICAL_DELIM_JOIN";
	} break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	{
		dsl_calcite = "LOGICAL_ANY_JOIN";
	} break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	{
		dsl_calcite = "LOGICAL_CROSS_PRODUCT";
	} break;
	case LogicalOperatorType::LOGICAL_UNION:
	{
		dsl_calcite = "LOGICAL_UNION";
	} break;
	case LogicalOperatorType::LOGICAL_EXCEPT:
	{
		dsl_calcite = "LOGICAL_EXCEPT";
	} break;
	case LogicalOperatorType::LOGICAL_INTERSECT:
	{
		dsl_calcite = "LOGICAL_INTERSECT";
	} break;
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
	{
		dsl_calcite = "LOGICAL_RECURSIVE_CTE";
	} break;
	case LogicalOperatorType::LOGICAL_INSERT:
	{
		dsl_calcite = "LOGICAL_INSERT";
	} break;
	case LogicalOperatorType::LOGICAL_DELETE:
	{
		dsl_calcite = "LOGICAL_DELETE";
	} break;
	case LogicalOperatorType::LOGICAL_UPDATE:
	{
		dsl_calcite = "LOGICAL_UPDATE";
	} break;
	case LogicalOperatorType::LOGICAL_ALTER:
	{
		dsl_calcite = "LOGICAL_ALTER";
	} break;
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	{
		dsl_calcite = "LOGICAL_CREATE_TABLE";
	} break;
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
	{
		dsl_calcite = "LOGICAL_CREATE_INDEX";
	} break;
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	{
		dsl_calcite = "LOGICAL_CREATE_SEQUENCE";
	} break;
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
	{
		dsl_calcite = "LOGICAL_CREATE_VIEW";
	} break;
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
	{
		dsl_calcite = "LOGICAL_CREATE_SCHEMA";
	} break;
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	{
		dsl_calcite = "LOGICAL_CREATE_MACRO";
	} break;
	case LogicalOperatorType::LOGICAL_DROP:
	{
		dsl_calcite = "LOGICAL_DROP";
	} break;
	case LogicalOperatorType::LOGICAL_PRAGMA:
	{
		dsl_calcite = "LOGICAL_PRAGMA";
	} break;
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	{
		dsl_calcite = "LOGICAL_TRANSACTION";
	} break;
	case LogicalOperatorType::LOGICAL_EXPLAIN:
	{
		dsl_calcite = "LOGICAL_EXPLAIN";
	} break;
	case LogicalOperatorType::LOGICAL_SHOW:
	{
		dsl_calcite = "LOGICAL_SHOW";
	} break;
	case LogicalOperatorType::LOGICAL_PREPARE:
	{
		dsl_calcite = "LOGICAL_PREPARE";
	} break;
	case LogicalOperatorType::LOGICAL_EXECUTE:
	{
		dsl_calcite = "LOGICAL_EXECUTE";
	} break;
	case LogicalOperatorType::LOGICAL_EXPORT:
	{
		dsl_calcite = "LOGICAL_EXPORT";
	} break;
	case LogicalOperatorType::LOGICAL_VACUUM:
	{
		dsl_calcite = "LOGICAL_VACUUM";
	} break;
	case LogicalOperatorType::LOGICAL_SET:
	{
		dsl_calcite = "LOGICAL_SET";
	} break;
	case LogicalOperatorType::LOGICAL_LOAD:
	{
		dsl_calcite = "LOGICAL_LOAD";
	} break;
	}

	printTabs();
	std::cout<<dsl_calcite<<std::endl;

	bool inner = false;
	for(auto& child : op->children){
		if(!inner) {
			level++;
			inner = true;
		}
		Imprimir(std::move(child));
	}
	if(inner) level--;
}

void ExecuteQuery(Connection &con, const string &query) {
	// create the logical plan
	unique_ptr<LogicalOperator> plan = con.ExtractPlan(query);
	plan->Print();

	Imprimir(std::move(plan));
	// transform the logical plan into our own plan
	//MyPlanGenerator generator;
	//auto my_plan = generator.TransformPlan(*plan);

	// execute the plan and print the result
	/*printf("Executing query: %s\n", query.c_str());
	printf("----------------------\n");
	vector<int> result;
	while (true) {
		result = my_plan->GetNextRow();
		if (result.empty()) {
			break;
		}
		string str;
		for (size_t i = 0; i < result.size(); i++) {
			if (i > 0) {
				str += ", ";
			}
			str += std::to_string(result[i]);
		}
		printf("%s\n", str.c_str());
	}
	printf("----------------------\n");*/
}

//===--------------------------------------------------------------------===//
// Table Scan Node
//===--------------------------------------------------------------------===//
class MyScanNode : public MyNode {
public:
	MyScanNode(string name_p, vector<column_t> column_ids_p) : name(move(name_p)), column_ids(move(column_ids_p)) {
		// fill up the data based on which table we are scanning
		if (name == "mytable") {
			// i
			data.push_back({1, 2, 3, 4, 5});
			// j
			data.push_back({2, 3, 4, 5, 6});
		} else if (name == "myothertable") {
			// k
			data.push_back({1, 10, 20});
		} else {
			throw std::runtime_error("Unsupported table!");
		}
	}

	string name;
	vector<column_t> column_ids;
	vector<vector<int>> data;
	int index = 0;

	vector<int> GetNextRow() override {
		vector<int> result;
		if (index >= data[0].size()) {
			return result;
		}
		// fill the result based on the projection list (column_ids)
		for (size_t i = 0; i < column_ids.size(); i++) {
			result.push_back(data[column_ids[i]][index]);
		}
		index++;
		return result;
	};
};

//===--------------------------------------------------------------------===//
// Expression Execution
//===--------------------------------------------------------------------===//

// note that we run expression execution directly on top of DuckDB expressions
// it is also possible to transform the expressions into our own expressions (MyExpression)
class MyExpressionExecutor {
public:
	MyExpressionExecutor(vector<int> current_row_p) : current_row(move(current_row_p)) {
	}

	vector<int> current_row;

	int Execute(Expression &expression);

protected:
	int Execute(BoundReferenceExpression &expr);
	int Execute(BoundCastExpression &expr);
	int Execute(BoundComparisonExpression &expr);
	int Execute(BoundConjunctionExpression &expr);
	int Execute(BoundConstantExpression &expr);
	int Execute(BoundFunctionExpression &expr);
};

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
class MyFilterNode : public MyNode {
public:
	MyFilterNode(unique_ptr<Expression> filter_node) : filter(move(filter_node)) {
	}

	unique_ptr<Expression> filter;

	bool ExecuteFilter(Expression &expr, const vector<int> &current_row) {
		MyExpressionExecutor executor(current_row);
		auto val = executor.Execute(expr);
		return val != 0;
	}

	vector<int> GetNextRow() override {
		D_ASSERT(child);
		while (true) {
			auto next = child->GetNextRow();
			if (next.empty()) {
				return next;
			}
			// check if the filter passes, if it does we return the row
			// if not we return the next row
			if (ExecuteFilter(*filter, next)) {
				return next;
			}
		}
	};
};

//===--------------------------------------------------------------------===//
// Projection
//===--------------------------------------------------------------------===//
class MyProjectionNode : public MyNode {
public:
	MyProjectionNode(vector<unique_ptr<Expression>> projections_p) : projections(move(projections_p)) {
	}

	vector<unique_ptr<Expression>> projections;

	vector<int> GetNextRow() override {
		auto next = child->GetNextRow();
		if (next.empty()) {
			return next;
		}
		MyExpressionExecutor executor(next);
		vector<int> result;
		for (size_t i = 0; i < projections.size(); i++) {
			result.push_back(executor.Execute(*projections[i]));
		}
		return result;
	};
};

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
class MyAggregateNode : public MyNode {
public:
	MyAggregateNode(vector<unique_ptr<Expression>> aggregates_p) : aggregates(move(aggregates_p)) {
		// initialize aggregate states to 0
		aggregate_states.resize(aggregates.size(), 0);
	}

	vector<unique_ptr<Expression>> aggregates;
	vector<int> aggregate_states;

	void ExecuteAggregate(MyExpressionExecutor &executor, int index, BoundAggregateExpression &expr) {
		if (expr.function.name == "sum") {
			int child = executor.Execute(*expr.children[0]);
			aggregate_states[index] += child;
		} else if (expr.function.name == "count_star") {
			aggregate_states[index]++;
		} else {
			throw std::runtime_error("Unsupported aggregate function " + expr.function.name);
		}
	}

	vector<int> GetNextRow() override {
		if (aggregate_states.empty()) {
			// finished aggregating
			return aggregate_states;
		}
		while (true) {
			auto next = child->GetNextRow();
			if (next.empty()) {
				return move(aggregate_states);
			}
			MyExpressionExecutor executor(next);
			for (size_t i = 0; i < aggregates.size(); i++) {
				ExecuteAggregate(executor, i, (BoundAggregateExpression &)*aggregates[i]);
			}
		}
	};
};

//===--------------------------------------------------------------------===//
// Plan Transformer - Transform a DuckDB logical plan into a custom plan (MyNode)
//===--------------------------------------------------------------------===//
unique_ptr<MyNode> MyPlanGenerator::TransformPlan(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// projection
		auto child = TransformPlan(*op.children[0]);
		auto node = make_unique<MyProjectionNode>(move(op.expressions));
		node->child = move(child);
		return move(node);
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		// filter
		auto child = TransformPlan(*op.children[0]);
		auto node = make_unique<MyFilterNode>(move(op.expressions[0]));
		node->child = move(child);
		return move(node);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = (LogicalAggregate &)op;
		if (!aggr.groups.empty()) {
			throw std::runtime_error("Grouped aggregate not supported");
		}
		auto child = TransformPlan(*op.children[0]);
		auto node = make_unique<MyAggregateNode>(move(op.expressions));
		node->child = move(child);
		return move(node);
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = (LogicalGet &)op;
		// table scan or table function

		// get nodes have two properties: table_filters (filter pushdown) and column_ids (projection pushdown)
		// table_filters are only generated if optimizers are enabled (through the filter pushdown optimizer)
		// column_ids are always generated
		// the column_ids specify which columns should be emitted and in which order
		// e.g. if we have a table "i, j, k" and the column_ids are {0, 2} we should emit ONLY "i, k" and in that order
		if (get.function.name == "seq_scan") {
			// built-in table scan
			auto &table = (TableScanBindData &)*get.bind_data;
			if (!get.table_filters.filters.empty()) {
				// note: filter pushdown will only be triggered if optimizers are enabled
				throw std::runtime_error("Filter pushdown unsupported");
			}
			return make_unique<MyScanNode>(table.table->name, get.column_ids);
		} else if (get.function.name == "my_scan") {
			// our own scan
			auto &my_bind_data = (MyBindData &)*get.bind_data;
			return make_unique<MyScanNode>(my_bind_data.table_name, get.column_ids);
		} else {
			throw std::runtime_error("Unsupported table function");
		}
	}
	default:
		throw std::runtime_error("Unsupported logical operator for transformation");
	}
}

//===--------------------------------------------------------------------===//
// Expression Execution for various built-in expressions
//===--------------------------------------------------------------------===//
int MyExpressionExecutor::Execute(BoundReferenceExpression &expr) {
	// column references (e.g. "SELECT a FROM tbl") are turned into BoundReferences
	// these refer to an index within the row they come from
	// because of that it is important to correctly handle the get.column_ids
	return current_row[expr.index];
}

int MyExpressionExecutor::Execute(BoundCastExpression &expr) {
	return Execute(*expr.child);
}

int MyExpressionExecutor::Execute(BoundConjunctionExpression &expr) {
	int result;
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		result = 1;
		for (size_t i = 0; i < expr.children.size(); i++) {
			result = result && Execute(*expr.children[i]);
		}
	} else if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		result = 0;
		for (size_t i = 0; i < expr.children.size(); i++) {
			result = result || Execute(*expr.children[i]);
		}
	} else {
		throw std::runtime_error("Unrecognized conjunction (this shouldn't be possible)");
	}
	return result;
}

int MyExpressionExecutor::Execute(BoundConstantExpression &expr) {
	return expr.value.GetValue<int32_t>();
}

int MyExpressionExecutor::Execute(BoundComparisonExpression &expr) {
	auto lchild = Execute(*expr.left);
	auto rchild = Execute(*expr.right);
	bool cmp;
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
		cmp = lchild == rchild;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		cmp = lchild != rchild;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		cmp = lchild < rchild;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		cmp = lchild > rchild;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		cmp = lchild <= rchild;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		cmp = lchild >= rchild;
		break;
	default:
		throw std::runtime_error("Unsupported comparison");
	}
	return cmp ? 1 : 0;
}

//===--------------------------------------------------------------------===//
// Expression Execution for built-in functions
//===--------------------------------------------------------------------===//
int MyExpressionExecutor::Execute(BoundFunctionExpression &expr) {
	if (expr.function.name == "+") {
		auto lchild = Execute(*expr.children[0]);
		auto rchild = Execute(*expr.children[1]);
		return lchild + rchild;
	}
	throw std::runtime_error("Unsupported function " + expr.function.name);
}

int MyExpressionExecutor::Execute(Expression &expression) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
		return Execute((BoundReferenceExpression &)expression);
	case ExpressionClass::BOUND_CAST:
		return Execute((BoundCastExpression &)expression);
	case ExpressionClass::BOUND_COMPARISON:
		return Execute((BoundComparisonExpression &)expression);
	case ExpressionClass::BOUND_CONJUNCTION:
		return Execute((BoundConjunctionExpression &)expression);
	case ExpressionClass::BOUND_CONSTANT:
		return Execute((BoundConstantExpression &)expression);
	case ExpressionClass::BOUND_FUNCTION:
		return Execute((BoundFunctionExpression &)expression);
	default:
		throw std::runtime_error("Unsupported expression for expression executor " + expression.ToString());
	}
}

int main() {
	//RunExampleDuckDBCatalog();
	//RunExampleTableScan();
	RunCustomFunctionDuckDB();
}



