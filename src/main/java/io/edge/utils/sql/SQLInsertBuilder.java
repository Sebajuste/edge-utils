package io.edge.utils.sql;

import java.util.ArrayList;
import java.util.List;

public class SQLInsertBuilder extends SQLSelectBuilder implements SQLBuilder {

	private String table;

	private final List<SQLItem> columnList = new ArrayList<>();

	private final List<SQLItem> valueList = new ArrayList<>();

	public SQLInsertBuilder schema(String schema) {
		super.schema(schema);
		return this;
	}

	public SQLInsertBuilder insertInto(String table) {
		this.table = table;
		return this;
	}

	public SQLInsertBuilder columns(String... columns) {
		for (String column : columns) {
			this.columnList.add(new SQLItem(column));
		}
		return this;
	}

	public SQLInsertBuilder values(Object... values) {
		for (Object value : values) {
			this.valueList.add(new SQLItem(value.toString()));
		}
		return this;
	}

	/*
	 * public SQLInsertBuilder select(SQLSelectBuilder selectBuilder) {
	 * this.selectBuilder = selectBuilder; return this; }
	 */

	@Override
	public String build() {
		StringBuilder builder = new StringBuilder();

		builder.append("INSERT INTO ").append(super.schema != null ? super.schema + "." + table : table).append(" ");

		builder.append(SQLBuildTool.buildList("(", columnList, ",", ")"));

		if (this.valueList.isEmpty()) {
			builder.append(super.build());
		} else {
			builder.append(SQLBuildTool.buildList("VALUES (", this.valueList, ",", ")"));
		}

		return builder.toString().trim();
	}

	@Override
	public String toString() {
		return this.build();
	}

}
