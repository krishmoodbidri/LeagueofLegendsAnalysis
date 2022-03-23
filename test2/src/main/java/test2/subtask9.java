package test2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

public class subtask9
{
    public static void main( String[] args ) throws Exception
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "winning match wrt gold");
	    job.setJarByClass(subtask9.class);
	    job.setMapperClass(st9Mapper.class);
	    job.setCombinerClass(st9Reducer.class);
	    job.setReducerClass(st9Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(20);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  	    
    }

    static class st9Mapper extends Mapper<Object, Text, Text, IntWritable> {
	
    	private Text champion = new Text();
    	private Text write = new Text();
    	private IntWritable experience;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String jsonString = value.toString();
			JSONObject obj = new JSONObject(jsonString);
			JSONObject obj1 = new JSONObject(jsonString);
			int winner = obj.getInt("winner");
			ArrayList<Integer> winList = new ArrayList<Integer>();
			ArrayList<Integer> loseList = new ArrayList<Integer>();
	
			
			JSONArray players = obj.getJSONArray("players");
			for (int i = 0; i < players.length(); i++) 
			{
				JSONObject o = players.getJSONObject(i);
				
				int teamID = o.getInt("teamID");
				int championID = o.getInt("championID");
				
				if(teamID==winner) winList.add(championID);
				else loseList.add(championID);
				
			}
			
			
			JSONObject data = (JSONObject)obj1.get("data");
			JSONArray experienceEarnedEvents = data.getJSONArray("experienceEarnedEvents");
			for (int j = 0; j < experienceEarnedEvents.length(); j++) 
				{
				
					JSONObject ob = experienceEarnedEvents.getJSONObject(j);
					int time = ob.getInt("time");
					if(time<=600)
					{
						int unitID = ob.getInt("unitID");
						int experienceEarned = ob.getInt("experienceEarned");
						experience= new IntWritable(experienceEarned);
						
						
						if(winList.contains(unitID))
						{
							
							champion.set("winning team");
							write.set(champion);
							context.write(write, experience);
						}
						else
						{
							
							champion.set("losing team");
							write.set(champion);
							context.write(write, experience);
						}
						
					}
				
				}
		}
	}

	static class st9Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable numGames = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			Integer experience = 0;
			
			for (IntWritable val : values) {
				experience =experience + val.get();
			}
			numGames.set(experience);
			context.write(key, numGames);
		}
	}    
}
