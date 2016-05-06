import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;


public class CreateTable {

	String filename;
	ArrayList<String> columnList =new ArrayList<>();
	String Schema;
	String columns[];
	
	CreateTable(String tableCreateStmt, String Schema)
	{
		filename=tableCreateStmt.substring(0,tableCreateStmt.indexOf("(")).trim().toLowerCase();
		this.Schema=Schema;
		columns=tableCreateStmt.substring(tableCreateStmt.indexOf("(")+1).split(",");
		
		for(int i=0; i<columns.length; i++)
		{
			columns[i]=columns[i].trim().toLowerCase();
			columnList.add(columns[i].substring(0,columns[i].indexOf(" ")).trim());
		}
	}
	
	public boolean checkTables() {
		boolean tableExists=false;
		try {

			RandomAccessFile InfoTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
			ArrayList<String> tableList = new ArrayList<>();
			
			while(true) {
				try{
			
			byte varcharLength = InfoTableFile.readByte();
			
			String schemaName="";
			char schemaArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
				schemaArr[i]=(char)InfoTableFile.readByte();
			
			schemaName=String.valueOf(schemaArr).trim();
			
			varcharLength = InfoTableFile.readByte();
			String TableName="";
			char tableArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
				tableArr[i]=(char)InfoTableFile.readByte();
			
			TableName=String.valueOf(tableArr).trim();
			Long table_rows=InfoTableFile.readLong();
			
			if(schemaName.equals(Schema))
			{
				tableList.add(TableName);
			}
			
				}catch(EOFException e) //Skip and break the reading loop incase of FileException
				{
					break;
				}catch(Exception e) //show in case of any other exception
				{
					e.printStackTrace();
				}
			}
			
			for (String table: tableList)
			{
				if(filename.equals(table))
				{
					tableExists=true;
				}
			}
			
		}
		catch(Exception e) {
			System.out.println(e);
					}
		
		return tableExists;
		}
	
	
	public void create()
	{
		createDataFile(Schema+"."+filename);
		
		for(String column:columnList)
		{
			createIndxFile(Schema+"."+filename+"."+column);
		}
		System.out.println("Created table "+filename);
	}
	
	public void createDataFile(String tableName)
	{
		String tableFileName=tableName.toLowerCase()+".tbl";
		RandomAccessFile widgetTableFile = null;
		try {
			widgetTableFile = new RandomAccessFile(tableFileName, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try
			{
			widgetTableFile.close();
			}catch(Exception e)
			{
				
			}
		}
		
	}
	
	public void createIndxFile(String tableName)
	{
		String tableFileName=tableName.toLowerCase()+".ndx";
		RandomAccessFile tableIdIndex=null;
		
		try
		{
			new FileOutputStream((tableFileName)).close();
		}catch(Exception e)
		{
			
		}
		
		try {
			
			tableIdIndex = new RandomAccessFile(tableFileName, "rw");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try
			{
				tableIdIndex.close();
			}catch(Exception e)
			{
				
			}
			
		}
	}
	
	public void updateInformationSchema()
	{
		try {
			
		RandomAccessFile InfoTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
		
		while(true)
		{
			try{
			InfoTableFile.readByte();
			}catch(EOFException e)
			{
				break;
			}
		}
		
		
		//writing the filename
		InfoTableFile.writeByte(Schema.length());
		InfoTableFile.writeBytes(Schema);
		InfoTableFile.writeByte(filename.length());
		InfoTableFile.writeBytes(filename);
		InfoTableFile.writeLong(0);
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		updateInfoColumnSchema();
	}
	
	
	public void updateInfoColumnSchema()
	{
		try {
			
			RandomAccessFile InfoColumnsFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			
			while(true) // reaching to the EOF
			{
				try{
				InfoColumnsFile.readByte();
				}catch(EOFException e)
				{
					break;
				}
			}
			
			for(int i=0;i<columns.length;i++)
			{
			InfoColumnsFile.writeByte(Schema.length());
			InfoColumnsFile.writeBytes(Schema);
			InfoColumnsFile.writeByte(filename.length());
			InfoColumnsFile.writeBytes(filename);
			
			String validTypes[]={"BYTE","SHORT","SHORT INT","INT","LONG","LONG INT","VARCHAR","CHAR","FLOAT","DOUBLE","DATETIME","DATE"};
			
				InfoColumnsFile.writeByte((columnList.get(i)).length());
				InfoColumnsFile.writeBytes(columnList.get(i)); //writing column
				InfoColumnsFile.writeInt(i+1); //ordinal position
				
				String testcmd= columns[i];
			for(String str2:validTypes)
			{
				if(testcmd.toLowerCase().contains(str2.toLowerCase()))
				{
					if(str2.equals("VARCHAR")||str2.equals("CHAR"))
					{
						testcmd=testcmd.toUpperCase();
						InfoColumnsFile.writeByte(testcmd.substring(testcmd.indexOf(str2),testcmd.indexOf(")")+1).length());
						InfoColumnsFile.writeBytes(testcmd.substring(testcmd.indexOf(str2),testcmd.indexOf(")")+1).toLowerCase());
					}
					else
					{
						InfoColumnsFile.writeByte(str2.toLowerCase().length());
						InfoColumnsFile.writeBytes(str2.toLowerCase());
					}
					break;
				}
			}
			
			//COLUMN_KEY condition
			if(testcmd.toLowerCase().contains("primary key")|| testcmd.toLowerCase().contains("not null"))
			{
				InfoColumnsFile.writeByte("NO".length());
				InfoColumnsFile.writeBytes("NO");
			}
			else
			{
				InfoColumnsFile.writeByte("YES".length());
				InfoColumnsFile.writeBytes("YES");
			}
			
			//write COLUMN_KEY if the column is Primary key
			if(testcmd.toLowerCase().contains("primary key"))
			{
				InfoColumnsFile.writeByte("PRI".length());
				InfoColumnsFile.writeBytes("PRI");
			}
		}
		
			updateInfoColumnRows(); //update no of rows for columns in info_schema.tables
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public void updateInfoColumnRows()
	{
		try {
		RandomAccessFile InfoTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
		
		while(true)
		{
		  try
		   {
			byte varcharLength = InfoTableFile.readByte();
			
			String schemaName="";
			char schemaArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
			{
				schemaArr[i]=(char)InfoTableFile.readByte();
				
			}
			//reading SchemaName
			schemaName=String.valueOf(schemaArr).trim();
			
			varcharLength = InfoTableFile.readByte();
			String tableName="";
			char tableArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
			{
				tableArr[i]=(char)InfoTableFile.readByte();
				
			}
			//reading TableName
			tableName=String.valueOf(tableArr).trim();
			
			long updPos= InfoTableFile.getFilePointer();
			long no_of_rows= InfoTableFile.readLong();
			
			
			if(tableName.equals("COLUMNS") && schemaName.equals("information_schema"))
			{
				InfoTableFile.seek(updPos); //go to the location before the number of rows
				no_of_rows=no_of_rows+columns.length;
				InfoTableFile.writeLong(no_of_rows);
			}
			
				
			}catch(EOFException e)
			{
				break;
			}
		}
		
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
}
