package sqlancer.noisepage.gen;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.noisepage.NoisePageErrors;
import sqlancer.noisepage.NoisePageProvider.NoisePageGlobalState;
import sqlancer.noisepage.NoisePageSchema;
import sqlancer.noisepage.NoisePageSchema.NoisePageColumn;
import sqlancer.noisepage.NoisePageSchema.NoisePageTable;
import sqlancer.noisepage.NoisePageToStringVisitor;
import sqlancer.gen.AbstractInsertGenerator;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class NoisePageInsertGenerator extends AbstractInsertGenerator<NoisePageColumn> {

    private final NoisePageGlobalState globalState;
    private final Set<String> errors = new HashSet<>();

    public NoisePageInsertGenerator(NoisePageGlobalState globalState) {
        this.globalState = globalState;
    }

    public static Query getQuery(NoisePageGlobalState globalState) throws SQLException {
        return new NoisePageInsertGenerator(globalState).generate();
    }

    private Query generate() throws SQLException {
        sb.append("INSERT INTO ");
        NoisePageTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<NoisePageColumn> temp = NoisePageSchema.getTableColumns(globalState.getConnection()
                , table.getName());
        List<NoisePageColumn> columns = new ArrayList<>();
        for(NoisePageColumn c: temp){
            double ran = Math.random();
            if (ran<0.5){
                columns.add(c);
            }
        }
        if (columns.size()==0){
            columns.add(temp.get(0));
        }
//        List<NoisePageColumn> columns = table.getRandomNonEmptyColumnSubset();
        for(NoisePageColumn c:columns){
            System.out.println("InsertGenerator columns: "+table.getName());
            System.out.println("InsertGenerator columns: "+c.getName());
            System.out.println("InsertGenerator columns: "+c.getType());
        }
        sb.append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        System.out.println("InsertGenerator: "+sb.toString());
        insertColumns(columns);
        System.out.println("InsertGenerator: "+sb.toString());
        NoisePageErrors.addInsertErrors(errors);
        return new QueryAdapter(sb.toString(), errors);
    }


    @Override
    protected void insertValue(NoisePageColumn tiDBColumn) {
        // TODO: select a more meaningful value
        System.out.println("Insert Generator tiDBColumn: "+tiDBColumn.getName()+tiDBColumn.getType().toString());
        sb.append(NoisePageToStringVisitor.asString(new NoisePageExpressionGenerator(globalState).generateConstant(tiDBColumn.getType())));
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            sb.append("DEFAULT");
//        } else {
//            sb.append(NoisePageToStringVisitor.asString(new NoisePageExpressionGenerator(globalState).generateConstant(tiDBColumn.getType())));
//        }
    }

}
