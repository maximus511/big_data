import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Format Genre class
 * @author Rahul Nair
 *
 */

public class GenreFormat extends EvalFunc <String> 
{
	@Override
	public String exec(Tuple input) 
	{
		try {
			if (input == null || input.size() == 0) 
			{
				return null;
			}

			String[] genres = ((String) input.get(0)).split("\\|");
			StringBuffer buf = new StringBuffer();
			int length = genres.length;
			int initialCount = 1;
			for(String value : genres)
			{
				if(initialCount == length)
				{
					buf = buf.append(value).append(" ").append("rkn130030");
				}

				else if (initialCount == length -1)
				{
					buf = buf.append(value).append(" ").append("&").append(" ");
				}

				else
				{
					buf = buf.append(value).append(" ").append(",").append(" ");
				}

				initialCount ++;
			}
			return buf.toString();


		} catch (ExecException ex) {
			System.out.println("Exception: " + ex.toString());
		}

		return null;
	}
}
