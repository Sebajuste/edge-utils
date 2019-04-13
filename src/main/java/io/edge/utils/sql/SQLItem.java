package io.edge.utils.sql;

import java.util.Objects;

public class SQLItem implements SQLBuilder {

	protected final String item;

	public SQLItem(String item) {
		this.item = Objects.requireNonNull(item);
	}

	@Override
	public String build() {
		return item;
	}

}
