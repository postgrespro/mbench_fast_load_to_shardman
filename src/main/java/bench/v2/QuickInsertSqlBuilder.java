package bench.v2;

public class QuickInsertSqlBuilder {

	public String getSQL(String prefix, int cntBinds, int nrows) {
		return buildSQL(prefix, cntBinds, nrows);
	}
	
	public static String buildSQL(String prefix, int cntBinds, int nrows) {
		StringBuffer buf = new StringBuffer();
		buf.append(prefix);
		buf.append(" values ");
		
		
		StringBuffer rowBinds = new StringBuffer();
		rowBinds.append("(");
		for (int i = 0; i < cntBinds; i++) {
			if (i != cntBinds - 1) {
				rowBinds.append("?,");
			} else {
				rowBinds.append("?");
			}
		}
		rowBinds.append(")");
		
		for (int i = 0; i < nrows; i++) {
			buf.append(rowBinds);
			if (i != nrows - 1) {
				buf.append(",");
			}
		}
		
		return buf.toString();
	}
}
