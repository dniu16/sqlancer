package sqlancer.noisepage;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.noisepage.NoisePageOptions.NoisePageOracleFactory;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.test.NoisePageNoRECOracle;
import sqlancer.noisepage.test.NoisePageQueryPartitioningAggregateTester;
import sqlancer.noisepage.test.NoisePageQueryPartitioningDistinctTester;
import sqlancer.noisepage.test.NoisePageQueryPartitioningGroupByTester;
import sqlancer.noisepage.test.NoisePageQueryPartitioningHavingTester;
import sqlancer.noisepage.test.NoisePageQueryPartitioningWhereTester;

@Parameters
public class NoisePageOptions implements DBMSSpecificOptions<NoisePageOracleFactory>{

    @Parameter(names = "--test-collate", arity = 1)
    public boolean testCollate = false;

    @Parameter(names = "--test-check", description = "Allow generating CHECK constraints in tables", arity = 1)
    public boolean testCheckConstraints = false;
//    public boolean testCheckConstraints = true;

    @Parameter(names = "--test-default-values", description = "Allow generating DEFAULT values in tables", arity = 1)
    public boolean testDefaultValues = true;

    @Parameter(names = "--test-not-null", description = "Allow generating NOT NULL constraints in tables", arity = 1)
    public boolean testNotNullConstraints = true;

    @Parameter(names = "--test-functions", description = "Allow generating functions in expressions", arity = 1)
    public boolean testFunctions = true;

    @Parameter(names = "--test-casts", description = "Allow generating casts in expressions", arity = 1)
    public boolean testCasts = false;
//    public boolean testCasts = true;

    @Parameter(names = "--test-between", description = "Allow generating the BETWEEN operator in expressions", arity = 1)
    public boolean testBetween = false;

    @Parameter(names = "--test-in", description = "Allow generating the IN operator in expressions", arity = 1)
    public boolean testIn = false;

    @Parameter(names = "--test-case", description = "Allow generating the CASE operator in expressions", arity = 1)
    public boolean testCase = false;

    @Parameter(names = "--test-binary-logicals", description = "Allow generating AND and OR in expressions", arity = 1)
    public boolean testBinaryLogicals = true;

    @Parameter(names = "--test-int-constants", description = "Allow generating INTEGER constants", arity = 1)
    public boolean testIntConstants = true;

    @Parameter(names = "--test-varchar-constants", description = "Allow generating VARCHAR constants", arity = 1)
    public boolean testStringConstants = true;

    @Parameter(names = "--test-date-constants", description = "Allow generating DATE constants", arity = 1)
    public boolean testDateConstants = true;

    @Parameter(names = "--test-timestamp-constants", description = "Allow generating TIMESTAMP constants", arity = 1)
    public boolean testTimestampConstants = true;

    @Parameter(names = "--test-float-constants", description = "Allow generating floating-point constants", arity = 1)
    public boolean testFloatConstants = true;

    @Parameter(names = "--test-boolean-constants", description = "Allow generating boolean constants", arity = 1)
    public boolean testBooleanConstants = true;

    @Parameter(names = "--test-binary-comparisons", description = "Allow generating binary comparison operators (e.g., >= or LIKE)", arity = 1)
    public boolean testBinaryComparisons = true;

    @Parameter(names = "--test-indexes", description = "Allow explicit (i.e. CREATE INDEX) and implicit (i.e., UNIQUE and PRIMARY KEY) indexes", arity = 1)
    public boolean testIndexes = true;

    @Parameter(names = "--test-rowid", description = "Test tables' rowid columns", arity = 1)
    public boolean testRowid = true;

    @Parameter(names = "--max-num-views", description = "The maximum number of views that can be generated for a database", arity = 1)
    public int maxNumViews = 1;

    @Parameter(names = "--max-num-deletes", description = "The maximum number of DELETE statements that are issued for a database", arity = 1)
    public int maxNumDeletes = 1;

    @Parameter(names = "--max-num-updates", description = "The maximum number of UPDATE statements that are issued for a database", arity = 1)
    public int maxNumUpdates = 5;

    @Parameter(names = "--oracle")
    public List<NoisePageOracleFactory> oracles = Arrays.asList(NoisePageOracleFactory.QUERY_PARTITIONING);

    public enum NoisePageOracleFactory implements OracleFactory<NoisePageGlobalState> {
        NOREC {

            @Override
            public TestOracle create(NoisePageGlobalState globalState) throws SQLException {
                return new NoisePageNoRECOracle(globalState);
            }

        },
        HAVING {
            @Override
            public TestOracle create(NoisePageGlobalState globalState) throws SQLException {
                return new NoisePageQueryPartitioningHavingTester(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle create(NoisePageGlobalState globalState) throws SQLException {
                return new NoisePageQueryPartitioningWhereTester(globalState);
            }
        },
        GROUP_BY {
            @Override
            public TestOracle create(NoisePageGlobalState globalState) throws SQLException {
                return new NoisePageQueryPartitioningGroupByTester(globalState);
            }
        },
        AGGREGATE {

            @Override
            public TestOracle create(NoisePageGlobalState globalState) throws SQLException {
                return new NoisePageQueryPartitioningAggregateTester(globalState);
            }

        },
        DISTINCT {
            @Override
            public TestOracle create(NoisePageGlobalState globalState) throws SQLException {
                return new NoisePageQueryPartitioningDistinctTester(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(NoisePageGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new NoisePageQueryPartitioningWhereTester(globalState));
                oracles.add(new NoisePageQueryPartitioningHavingTester(globalState));
                oracles.add(new NoisePageQueryPartitioningAggregateTester(globalState));
                oracles.add(new NoisePageQueryPartitioningDistinctTester(globalState));
                oracles.add(new NoisePageQueryPartitioningGroupByTester(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };
    }

    @Override
    public List<NoisePageOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
