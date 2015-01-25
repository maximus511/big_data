import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class HiveGenreFormat extends UDF 
{

	public Text evaluate(Text s) 
	{
		String input = null;
		if (s != null) 
		{
			try 
			{
				input =  s.toString();
				String[] genreValues = input.split("\\|");
				StringBuffer buf = new StringBuffer();
				int initialCount = 1;
				int length = genreValues.length;
				for(String value : genreValues)
				{
					if(initialCount == length)
					{
						buf = buf.append(value).append(" ").append("rkn130030");
					}

					if(initialCount == length -1)
					{
						buf = buf.append(value).append("&").append(' ');
					}

					else
					{
						buf = buf.append(",").append(" ");
					}

					initialCount ++;
				}
				return new Text(buf.toString());

			} 
			catch (Exception e) 
			{ 
				System.out.println("Error: " + e.toString());
			}
			return null;
		}
		return null;
	}
}
