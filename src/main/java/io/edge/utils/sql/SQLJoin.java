package io.edge.utils.sql;

public class SQLJoin implements SQLBuilder {

	public enum Type {
		INNER("INNER JOIN"),
		LEFT_OUTER("LEFT OUTER JOIN"),
		RIGHT_OUTER("RIGHT OUTER JOIN");

		private final String sql;

		Type(String sql) {
			this.sql = sql;
		}

		protected String sql() {
			return this.sql;
		}

	}

	private final Type type;

	private final String table;

	private final String on;

	public SQLJoin(Type type, String table, String on) {
		super();
		this.type = type;
		this.table = table;
		this.on = on;
	}

	public Type getType() {
		return type;
	}

	public String getTable() {
		return table;
	}

	public String getOn() {
		return on;
	}

	@Override	
	public String build() {
		return this.type.sql + " " + this.table + " ON " + this.on;
	}

}
