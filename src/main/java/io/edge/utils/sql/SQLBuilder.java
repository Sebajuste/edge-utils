package io.edge.utils.sql;

@FunctionalInterface
public interface SQLBuilder {

	String build();

	static SQLSelectBuilder createSelect() {
		return new SQLSelectBuilder();
	}

	static SQLInsertBuilder createInsert() {
		return new SQLInsertBuilder();
	}

	static SQLUpdateBuilder createUpdate() {
		return new SQLUpdateBuilder();
	}

	static SQLDeleteBuilder createDelete() {
		return new SQLDeleteBuilder();
	}

}
