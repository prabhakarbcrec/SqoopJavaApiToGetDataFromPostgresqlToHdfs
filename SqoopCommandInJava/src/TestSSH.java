import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

/**
 * 
 * @author P.K. Ojha
 *
 */


public class TestSSH {
	
	static ArrayList<String> StartDate=new ArrayList<String>(Arrays.asList("2017-01-01 00:00:00","2017-02-01 00:00:00","2017-03-01 00:00:00","2017-04-01 00:00:00","2017-05-01 00:00:00","2017-06-01 00:00:00","2017-07-01 00:00:00","2017-08-01 00:00:00","2017-09-01 00:00:00","2017-10-01 00:00:00","2017-11-01 00:00:00","2017-12-01 00:00:00"));
	static ArrayList<String> Enddate=new ArrayList<String>(Arrays.asList("2017-01-31 23:59:59","2017-02-28 23:59:59","2017-03-31 23:59:59","2017-04-30 23:59:59","2017-05-31 23:59:59","2017-06-30 23:59:59","2017-07-31 23:59:59","2017-08-31 23:59:59","2017-09-30 23:59:59","2017-10-31 23:59:59","2017-11-30 23:59:59","2017-12-31 23:59:59"));
	
	

	static ArrayList<String> monthData=new ArrayList<String>(Arrays.asList("JAN_2017","FEB_2017","MAR_2017","APR_2017","MAY_2017","JUN_2017","JUL_2017","AUG_2017","SEPT_2017","OCT_2017","NOV_2017","DEC_2017"));
	
	public static int i;
	
	
	public static String sql;
	public static void main(String[] prabhakar) throws TaskExecFailException
	{	
		
		for(i=0;i<=11;i++)
		{
			

		sql="select distinct * from data2017 where BaseDateTime between "+"\'"+StartDate.get(i)+"\'"+" :: timestamp "+"AND "+"\'"+Enddate.get(i)+"\'"+":: timestamp and \\$CONDITIONS";
		
		System.out.println("\""+sql+"\""+"\n"+"");
              Main(sql,monthData.get(i));
		}
		
	}
	private static void Main(String sql2,String FileName) throws TaskExecFailException {
		
		// TODO Auto-generated method stub
		
		System.out.println("Here, We can put it our code: Sqoop Connection");
	    ConnBean cb = new ConnBean("IpOFSQOOPInstalledMAchine", "USERNAMEOFTHATMACHINE","PASSWORDOFTHATMACHINE");
	    SSHExec ssh = SSHExec.getInstance(cb);  
	    ssh.connect();
	    CustomTask sampleTask1 = new ExecCommand("echo $SSH_CLIENT"); 
	    System.out.println(ssh.exec(sampleTask1));
	    CustomTask sampleTask2 = new ExecCommand("sqoop-import -Dmapreduce.job.user.classpath.first=true -Dsqoop.export.records.per.statement=1 --connect jdbc:postgresql://IPofDataBase:5432/DBName --username postgres --password postgres --query \"select id::varchar from tbl_track_list_dtl where sensor_timestamp between '1571723776' AND '1571724182' and  \\$CONDITIONS\" --as-avrodatafile --compression-codec snappy --target-dir /2015/new.avro -m 1 --append"); 	
	    //CustomTask sampleTask2 = new ExecCommand("sqoop-import -Dmapreduce.job.user.classpath.first=true -Dsqoop.export.records.per.statement=1 --connect jdbc:postgresql://192.168.11.130:5432/FinalData --username postgres --password postgres --query "+"\" "+sql+"\""+" --as-avrodatafile --compression-codec snappy --target-dir /2017/"+FileName+" -m 1");
	    ssh.exec(sampleTask2);
	    ssh.disconnect(); 
        ssh.disconnect(); 
	    }
	
	}


