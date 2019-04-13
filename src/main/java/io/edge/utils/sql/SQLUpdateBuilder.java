package io.edge.utils.sql;

import java.util.ArrayList;
import java.util.List;

import io.edge.utils.sql.SQLOperand.Operator;

public class SQLUpdateBuilder implements SQLBuilder {

	private final List<SQLItem> setList = new ArrayList<>();

	private final List<SQLBuilder> whereList = new ArrayList<>();

	private String schema;

	private String table;

	public SQLUpdateBuilder schema(String schema) {
		this.schema = schema;
		return this;
	}

	public String schema() {
		return this.schema;
	}

	public SQLUpdateBuilder update(String table) {
		this.table = table;
		return this;
	}

	public SQLUpdateBuilder set(String... columns) {
		for (String column : columns) {
			this.setList.add(new SQLItem(column));
		}
		return this;
	}

	public SQLUpdateBuilder where(String item) {
		this.whereList.add(new SQLItem(item));
		return this;
	}

	public SQLUpdateBuilder and(String item) {
		this.whereList.add(new SQLOperand(Operator.AND, item));
		return this;
	}

	public SQLUpdateBuilder or(String item) {
		this.whereList.add(new SQLOperand(Operator.OR, item));
		return this;
	}

	@Override
	public String build() {
		StringBuilder builder = new StringBuilder();

		builder.append("UPDATE ").append(this.schema != null ? this.schema + "." + this.table : this.table).append(" ");

		builder.append(SQLBuildTool.buildList("SET", this.setList, ","));

		builder.append(SQLBuildTool.buildList("WHERE", this.whereList));

		return builder.toString().trim();
	}

	@Override
	public String toString() {
		return this.build();
	}

}
