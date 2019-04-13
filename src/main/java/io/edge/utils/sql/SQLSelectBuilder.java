package io.edge.utils.sql;

import java.util.ArrayList;
import java.util.List;

import io.edge.utils.sql.SQLJoin.Type;
import io.edge.utils.sql.SQLOperand.Operator;

public class SQLSelectBuilder implements SQLBuilder {

	private enum Condition {
		WHERE, HAVING;
	}

	private final List<SQLItem> selectList = new ArrayList<>();

	private final List<SQLFrom> fromList = new ArrayList<>();

	private final List<SQLBuilder> whereList = new ArrayList<>();

	private final List<SQLOrder> orderList = new ArrayList<>();

	private final List<SQLItem> groupByList = new ArrayList<>();

	private final List<SQLBuilder> havingList = new ArrayList<>();

	protected String schema;

	private int limit = -1;

	private int offset = -1;

	private SQLFrom currentfrom;

	private Condition currentCondition;

	private List<SQLBuilder> currentConditionList() {
		switch (this.currentCondition) {
			case HAVING:
				return this.havingList;
			case WHERE:
			default:
				return this.whereList;
		}
	}

	public SQLSelectBuilder schema(String schema) {
		this.schema = schema;
		return this;
	}

	public SQLSelectBuilder select(String... columns) {
		for (String column : columns) {
			this.selectList.add(new SQLItem(column));
		}
		return this;
	}

	public SQLSelectBuilder select() {
		this.selectList.add(new SQLItem("*"));
		return this;
	}

	public SQLSelectBuilder from(String... tables) {
		for (String table : tables) {
			this.currentfrom = new SQLFrom(this.schema != null ? this.schema + "." + table : table);
			this.fromList.add(this.currentfrom);
		}
		return this;
	}

	public SQLSelectBuilder innerJoin(String table, String on) {
		this.currentfrom.join(Type.INNER, this.schema != null ? this.schema + "." + table : table, on);
		return this;
	}

	public SQLSelectBuilder leftOuterJoin(String table, String on) {
		this.currentfrom.join(Type.LEFT_OUTER, this.schema != null ? this.schema + "." + table : table, on);
		return this;
	}

	public SQLSelectBuilder rightOuterJoin(String table, String on) {
		this.currentfrom.join(Type.RIGHT_OUTER, this.schema != null ? this.schema + "." + table : table, on);
		return this;
	}

	public SQLSelectBuilder where(String item) {
		this.currentCondition = Condition.WHERE;
		this.whereList.add(new SQLItem(item));
		return this;
	}

	public SQLSelectBuilder whereNotExists(SQLSelectBuilder selectBuilder) {
		this.currentCondition = Condition.WHERE;
		this.whereList.add(() -> "NOT EXISTS ( " + selectBuilder + " ) ");
		return this;
	}

	public SQLSelectBuilder having(String item) {
		this.currentCondition = Condition.HAVING;
		this.havingList.add(new SQLItem(item));
		return this;
	}

	public SQLSelectBuilder and(String item) {
		this.currentConditionList().add(new SQLOperand(Operator.AND, item));
		return this;
	}

	public SQLSelectBuilder or(String item) {
		this.currentConditionList().add(new SQLOperand(Operator.OR, item));
		return this;
	}

	public SQLSelectBuilder groupBy(String... columns) {
		for (String column : columns) {
			this.groupByList.add(new SQLItem(column));
		}
		return this;
	}

	public SQLSelectBuilder orderBy(String... columns) {
		for (String column : columns) {
			this.orderList.add(new SQLOrder(column));
		}
		return this;
	}

	public SQLSelectBuilder limit(int limit) {
		this.limit = limit;
		return this;
	}

	public SQLSelectBuilder offset(int offset) {
		this.offset = offset;
		return this;
	}

	@Override
	public String build() {

		StringBuilder builder = new StringBuilder();

		builder.append(SQLBuildTool.buildList("SELECT", this.selectList, ","));

		builder.append(SQLBuildTool.buildList("FROM", this.fromList, ","));

		builder.append(SQLBuildTool.buildList("WHERE", this.whereList));

		builder.append(SQLBuildTool.buildList("GROUP BY", this.groupByList, ","));

		builder.append(SQLBuildTool.buildList("HAVING", this.havingList));

		builder.append(SQLBuildTool.buildList("ORDER BY", this.orderList, ","));

		if (this.limit > 0) {
			builder.append("LIMIT ").append(this.limit).append(" ");
		}

		if (this.offset > 0) {
			builder.append("OFFSET ").append(this.offset).append(" ");
		}

		return builder.toString().trim();
	}

	@Override
	public String toString() {
		return this.build();
	}

}
