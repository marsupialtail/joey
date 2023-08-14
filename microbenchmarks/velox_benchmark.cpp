#include <folly/init/Init.h>
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <chrono>

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

class VeloxIn10MinDemo : public VectorTestBase {
 public:
  const std::string kTpchConnectorId = "test-tpch";

  VeloxIn10MinDemo() {
    // Register Presto scalar functions.
    functions::prestosql::registerAllScalarFunctions();

    // Register Presto aggregate functions.
    aggregate::prestosql::registerAllAggregateFunctions();

    // Register type resolver with DuckDB SQL parser.
    parse::registerTypeResolver();

    // Register TPC-H connector.
    auto tpchConnector =
        connector::getConnectorFactory(
            connector::tpch::TpchConnectorFactory::kTpchConnectorName)
            ->newConnector(kTpchConnectorId, nullptr);
    connector::registerConnector(tpchConnector);
  }

  ~VeloxIn10MinDemo() {
    connector::unregisterConnector(kTpchConnectorId);
  }

  /// Parse SQL expression into a typed expression tree using DuckDB SQL parser.
  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(text, options);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  /// Compile typed expression tree into an executable ExprSet.
  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  /// Evaluate an expression on one batch of data.
  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, context, result);
    return result[0];
  }

  /// Make TPC-H split to add to TableScan node.
  exec::Split makeTpchSplit() const {
    return exec::Split(std::make_shared<connector::tpch::TpchConnectorSplit>(
        kTpchConnectorId));
  }

  /// Run the demo.
  void run();

  std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};
  std::shared_ptr<core::QueryCtx> queryCtx_{
      std::make_shared<core::QueryCtx>(executor_.get())};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

void VeloxIn10MinDemo::run() {
  // Let’s create two vectors of 64-bit integers and one vector of strings.

  auto n_rows = std::vector<int64_t>({1,10,100,1000,10000,100000,1000000});
  auto total_cols = 30;
  auto n_cols = std::vector<int64_t>({1,2,4,8,16});

  std::vector<VectorPtr> cols = {};
  std::vector<std::string> col_names = {};

  for (const auto & n_row : n_rows) {
    for (auto i = 0; i < total_cols; i++) {
        // generate a vector of n_rows[0] random integers from 0 to 1000
        auto col = makeFlatVector<int64_t>(n_row, [&](auto row) {
        return random() % 1000;
        });
        cols.push_back(col);
        col_names.push_back("col_" + std::to_string(i));
    }

     auto data = makeRowVector(col_names, cols);
    //  std::cout << std::endl
    //         << "> data: " << data->toString() << std::endl;
    // std::cout << data->toString(0, 5) << std::endl;

    for(const auto & n_col : n_cols) {

        std::vector<std::string> selected_col_names = {};
        for (auto i = 0; i < n_col; i++) {
            selected_col_names.push_back("col_" + std::to_string(i));
        }

        std::string predicate;

        for (auto i = 0; i < n_col; i ++) {
            predicate += "col_" + std::to_string(i) + " > 500";
            if (i != n_col - 1) {
                predicate += " AND ";
            }
        }

        auto plan = PlanBuilder()
                        .values({data})
                        .filter(predicate)
                        .project(selected_col_names)
                        .planNode();

        auto sumAvg = AssertQueryBuilder(plan).copyResults(pool());
        auto start_time = std::chrono::high_resolution_clock::now();
        for(int i = 0; i < 1000; i ++){
            auto sumAvg = AssertQueryBuilder(plan).copyResults(pool());
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "Time taken for " << n_row << " rows and " << n_col << " cols: " << duration.count() << " ms" << std::endl;

        // std::cout << std::endl
        //             << "> sum and average for a and b: " << sumAvg->toString()
        //             << std::endl;
        // std::cout << sumAvg->toString(0, 5) << std::endl;
    }
  }
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);

  VeloxIn10MinDemo demo;
  demo.run();
}