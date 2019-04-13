package io.edge.utils.sql;

import java.util.Collection;

public final class SQLBuildTool {

	private SQLBuildTool() {

	}

	static protected String buildList(String prefix, Collection<? extends SQLBuilder> queryList, String separated, String postfix) {

		if (queryList.isEmpty()) {
			return "";
		}

		StringBuilder builder = new StringBuilder();

		if (prefix != null) {
			builder.append(prefix).append(" ");
		}

		int index = 0;
		for (SQLBuilder query : queryList) {
			builder.append(query.build().trim());

			if (separated != null && index < queryList.size() - 1) {
				builder.append(separated);
			}
			builder.append(" ");

			++index;
		}

		if (postfix != null) {
			builder.append(postfix).append(" ");
		}

		return builder.toString();
	}

	static protected String buildList(String prefix, Collection<? extends SQLBuilder> queryList, String separated) {
		return SQLBuildTool.buildList(prefix, queryList, separated, null);
	}

	static protected String buildList(String prefix, Collection<? extends SQLBuilder> queryList) {
		return SQLBuildTool.buildList(prefix, queryList, null, null);
	}

	static protected String buildList(Collection<? extends SQLBuilder> queryList) {
		return SQLBuildTool.buildList(null, queryList, null, null);
	}

	static protected String buildList(Collection<? extends SQLBuilder> queryList, String separated) {
		return SQLBuildTool.buildList(null, queryList, separated, null);
	}

}
