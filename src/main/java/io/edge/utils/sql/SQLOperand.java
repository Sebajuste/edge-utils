package io.edge.utils.sql;

import java.util.Objects;

public class SQLOperand extends SQLItem {

	public enum Operator {
		AND, OR
	}

	private final Operator operator;

	public SQLOperand(String item) {
		super(item);
		this.operator = null;
	}

	public SQLOperand(Operator operator, String item) {
		super(item);
		this.operator = Objects.requireNonNull(operator);
	}

	@Override
	public String build() {
		return operator == null ? super.build() : operator.name() + " " + super.item;
	}

}
