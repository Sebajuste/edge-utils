package io.edge.utils.sql;

public class SQLOrder implements SQLBuilder {

	public enum Type {
		ASC, DESC
	}

	private final Type type;

	private final String column;

	public SQLOrder(Type type, String column) {
		super();
		this.type = type;
		this.column = column;
	}
	
	public SQLOrder(String column) {
		super();
		this.type = Type.ASC;
		this.column = column;
	}

	@Override
	public String build() {
		return this.column + " " + this.type.name();
	}

}
