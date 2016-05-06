import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Scanner;


public class SriniBase {

	static String widgetTableFileName="";
	static String prompt = "davisql> ";
	static String currentSchema="information_schema";
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//Firstly, run the MakeInformationSchema to get the information_schema tables to be built
		
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		String userCommand; // Variable to collect user input from the prompt
		
		splashScreen();
		
		do {  // do-while !exit
			System.out.print(prompt);
			userCommand = scanner.next().trim();
			
			String systemCommand=checkUserCommand(userCommand);
			
		switch (systemCommand) {
		
		case "show schemas": // display all records from schemata table
			showSchemas();
			//displayAllRecords();
			break;
			
		case "display":
			/* 
			 *  Your record retrieval must use the SELECT-FROM-WHERE syntax
			 *  This simple syntax allows retrieval of single records based 
			 *  only on the ID column.
			 */
			//String recordID = scanner.next().trim();
			//displayRecordID(Integer.parseInt(recordID));
			break;
			
		case "use":
			
			if(changeSchema(userCommand.substring("use".length()).trim()))
			{
				System.out.println("Current schema changed to "+currentSchema);
			}
			else
				System.out.println("Schema not changed");
			
			break;
			
		case "create schema":
			/*
			 * Here we have to add an entry to the schemata table in the information
			 */
			if(addSchema(userCommand.substring("create schema".length()).trim()))
					{
				System.out.println("New Schema created");
					}
			else
				System.out.println("Schema not created");
			
			break;
			
		case "show tables":
			showTables();
			break;
			
		case "help":
			help();
			break;
			
		case "create table":
			CreateTable newTable= new CreateTable(userCommand.substring("create table".length()).trim(),currentSchema);
			if(!newTable.checkTables())
			{
			newTable.create();
			newTable.updateInformationSchema();
			}
			else
			{
				System.out.println("Table with the given name already exists in this schema");
			}
			break;
			
		case "version":
			version();
			break;
			
		case "insert into":
			InsertTable it=new InsertTable(userCommand.substring("insert into".length()).trim(),currentSchema);
			if(it.gettingColumns())
			{
				if(it.gettingCount()==false) //checking for misMatch of rows
				{
						if(it.checkPrimaryKey())
						{
							System.out.println("Same Primary Key value exists in the system");
						}
						else
						{
							if(it.insertIntoTbl())
							{
								System.out.println("Inserted 1 row");
							}
							else
							{
							 System.out.println("Column NOT Null or Primarykey condition not satisfied. Row not inserted");
							}
						}
				}
				else
				{
					System.out.println("Format Error or Row Count error. Row not inserted");
				}
				
			}
			else
			{
			System.out.println("Unable to find the table in the current schema");
			}
			break;
			
		case "select *":
			SelectQuery sq=new SelectQuery(userCommand.substring("select * from".length()).trim(),currentSchema);
			if(sq.gettingColumns())
			{
				sq.findInitialAddress();
				sq.display();
			}
			else
			{
				System.out.println("No table found in this schema");
			}
			break;
			
		case "drop table":
			DropTable dp= new DropTable(userCommand.substring("drop table".length()).trim(),currentSchema);
			 if(dp.findTable())
			 {
				 dp.storeTableContents();
				 System.out.println("Table Found for dropping");
				 dp.writeNewTableFile();
				 dp.storeColumnContents();
				 System.out.println("Columns Found for dropping");
				 dp.writeNewColumFile();
				 dp.deleteFiles();
			 }
			 else
			 {
				 System.out.println("Table not found in the current schema");
			 }
			break;
			
		case "exit":
			//simply exit
			break;
			
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
		}
		} while(!userCommand.equals("exit"));
		System.out.println("Exiting...");

	} /* End main() method */

	
	/*Check validity of the command */
	public static String checkUserCommand(String testcmd)
	{
		String usrcmd="";
		String valid_Commands[]={"SHOW SCHEMAS","USE","SHOW TABLES","CREATE SCHEMA","CREATE TABLE","INSERT INTO","DROP TABLE","EXIT","SELECT *"};
		boolean valid=false;
		
		for(String str2:valid_Commands)
		{
			if(testcmd.toLowerCase().contains(str2.toLowerCase()))
			{
				usrcmd=str2.toLowerCase();
				valid=true;
			}
		}
		
		if(valid==false)
		{
			usrcmd=testcmd;
		}
		
		return usrcmd;
	}
	
	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*",80));
		System.out.println();
		System.out.println("\tdisplay all;   Display all records in the table.");
		System.out.println("\tdisplay; <id>;     Display records whose ID is <id>.");
		System.out.println("\tversion;       Show the program version.");
		System.out.println("\thelp;          Show this help information");
		System.out.println("\texit;          Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}
	
	/**
	 *  Display the welcome "splash screen"
	 */
	public static void splashScreen() {
		System.out.println(line("*",80));
        System.out.println("Welcome to SriniBaseLite"); // Display the string.
		version();
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("*",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void version() {
		System.out.println("SriniBaseLite v1.0\n");
	}
	
	
	public static void showSchemas() {
		RandomAccessFile widgetTableFile =null;
		try {

			widgetTableFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");

			while(true) {
				try{
			byte varcharLength = widgetTableFile.readByte();
			System.out.print("\t");
			for(int i = 0; i < varcharLength; i++)
				System.out.print((char)widgetTableFile.readByte());
			System.out.println();
				}catch(EOFException e) //Skip and break the reading loop incase of FileException
				{
					break;
				}catch(Exception e) //show in case of any other exception
				{
					e.printStackTrace();
				}
			}
		}
		catch(Exception e) {
			System.out.println(e);
		}
		finally
		{
			try {
				widgetTableFile.close();
			} catch (IOException e) {
				
			}
		}
	}
	
	
	public static boolean addSchema(String newSchema)
	{
		boolean schemaCreated=false,nameExists=false;
		RandomAccessFile schemataTableFile=null;
		RandomAccessFile InfoTableFile =null;
		RandomAccessFile InfoTableFile2 = null;
		try
		{
		schemataTableFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");
		InfoTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
		InfoTableFile2 = new RandomAccessFile("information_schema.tables.tbl", "rw");
		//check for the already existing schema names
		while(true) {
			try{
		byte varcharLength = schemataTableFile.readByte();
		String schemaName="";
		char schemaArr[]=new char[30];
		for(int i = 0; i < varcharLength; i++)
			schemaArr[i]=(char)schemataTableFile.readByte();
		
		schemaName=String.valueOf(schemaArr).trim();
		
		if(newSchema.equals(schemaName))
		{
			nameExists=true;
		}
		
			}catch(EOFException e) //Skip and break the reading loop incase of FileException
			{
				break;
			}catch(Exception e) //show in case of any other exception
			{
				e.printStackTrace();
			}
		}
		
		if(nameExists==false)
		{
		
			schemataTableFile.writeByte(newSchema.length());
			schemataTableFile.writeBytes(newSchema);
			
			//update the information_schema.tables.tbl
			byte varcharLength = InfoTableFile.readByte();
			InfoTableFile2.readByte();
			
			String schemaName="";
			char schemaArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
			{
				schemaArr[i]=(char)InfoTableFile.readByte();
				InfoTableFile2.readByte();
			}
			schemaName=String.valueOf(schemaArr).trim();
			
			varcharLength = InfoTableFile.readByte();
			InfoTableFile2.readByte();
			
			String TableName="";
			char tableArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
			{
				tableArr[i]=(char)InfoTableFile.readByte();
				InfoTableFile2.readByte();
			}
			TableName=String.valueOf(tableArr).trim();
			
			Long table_rows=InfoTableFile.readLong();
			//once we read until schemata, we update the same
			if(TableName.equals("SCHEMATA"))
				InfoTableFile2.writeLong(++table_rows);
			
			schemaCreated=true;
		}
		else
		{
			System.out.println("Schema with the given name already exists");
		}
		
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
		finally
		{
			try
			{
			schemataTableFile.close();
			InfoTableFile.close();
			InfoTableFile2.close();
			}catch(IOException e)
			{
				
			}
		}
		
		return schemaCreated;
	}


public static boolean changeSchema(String changeSchema)
{
	boolean schemaChanged=false,nameExists=false;
	RandomAccessFile schemataTableFile=null;
	try
	{
	 schemataTableFile= new RandomAccessFile("information_schema.schemata.tbl", "rw");
	
	//check for the already existing schema names
	while(true) {
		try{
	byte varcharLength = schemataTableFile.readByte();
	String schemaName="";
	char schemaArr[]=new char[30];
	for(int i = 0; i < varcharLength; i++)
		schemaArr[i]=(char)schemataTableFile.readByte();
	
	schemaName=String.valueOf(schemaArr).trim();
	
	if(changeSchema.equals(schemaName))
	{
		nameExists=true;
	}
	
		}catch(EOFException e) //Skip and break the reading loop incase of FileException
		{
			break;
		}catch(Exception e) //show in case of any other exception
		{
			e.printStackTrace();
		}
	}
	
	if(nameExists==true)
	{
		currentSchema=changeSchema;
		schemaChanged=true;
		
	}
	else
	{
		System.out.println("Schema with the given name doesn't exist in the system");
	}
	
	}catch(Exception e)
	{
		e.printStackTrace();
	}
	
	finally
	{
		try {
			schemataTableFile.close();
		} catch (IOException e) {
			
		}
	}
	
	return schemaChanged;
	}


public static void showTables() {
	
	RandomAccessFile InfoTableFile=null;
	
	try {

		InfoTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
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
		
		if(schemaName.equals(currentSchema))
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
			System.out.println("\t"+table);
		}
		
	}
	catch(Exception e) {
		System.out.println(e);
				}
	
	finally
	{
		try {
			InfoTableFile.close();
		} catch (IOException e) {
			
		}
	}
	}
}