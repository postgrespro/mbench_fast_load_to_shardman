package bench.v2;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PrepareStatement {

	void bind(PreparedStatement pstmt) throws SQLException;
	
}
