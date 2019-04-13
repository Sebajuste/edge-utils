package io.edge.utils.sql;

import java.util.ArrayList;
import java.util.List;

import io.edge.utils.sql.SQLOperand.Operator;

public class SQLDeleteBuilder implements SQLBuilder {

	private final List<SQLBuilder> whereList = new ArrayList<>();

	private String schema;

	private String table;

	public SQLDeleteBuilder delete(String table) {
		this.table = table;
		return this;
	}

	public SQLDeleteBuilder schema(String schema) {
		this.schema = schema;
		return this;
	}

	public String schema() {
		return this.schema;
	}

	public SQLDeleteBuilder where(String item) {
		this.whereList.add(new SQLItem(item));
		return this;
	}

	public SQLDeleteBuilder and(String item) {
		this.whereList.add(new SQLOperand(Operator.AND, item));
		return this;
	}

	public SQLDeleteBuilder or(String item) {
		this.whereList.add(new SQLOperand(Operator.OR, item));
		return this;
	}

	@Override
	public String build() {
		StringBuilder builder = new StringBuilder();

		builder.append("DELETE FROM ").append(this.schema != null ? this.schema + "." + this.table : this.table).append(" ");

		builder.append(SQLBuildTool.buildList("WHERE", this.whereList));

		return builder.toString().trim();
	}

	@Override
	public String toString() {
		return this.build();
	}

}
