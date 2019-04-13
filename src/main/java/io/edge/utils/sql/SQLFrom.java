package io.edge.utils.sql;

import java.util.ArrayList;
import java.util.List;

public class SQLFrom implements SQLBuilder {

	private final List<SQLJoin> joinList = new ArrayList<>();

	private final String tableFrom;

	public SQLFrom(String tableFrom) {
		super();
		this.tableFrom = tableFrom;
	}

	public SQLFrom join(SQLJoin.Type type, String table, String on) {
		this.joinList.add(new SQLJoin(type, table, on));
		return this;
	}

	@Override
	public String build() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.tableFrom).append(" ");
		builder.append(SQLBuildTool.buildList(this.joinList));
		return builder.toString();
	}

}
