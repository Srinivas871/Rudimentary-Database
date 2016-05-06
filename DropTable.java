import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.lang.model.type.NullType;


public class DropTable {

	String DrpTableName;
	String Schema;
	TreeMap<Object, ArrayList> tableIndex;
	TreeMap<Object, ArrayList> columnIndex;
	ArrayList<String> columnList =new ArrayList<>();
	
	public DropTable(String DrpTableName, String Schema)
	{
		this.DrpTableName=DrpTableName.trim().toLowerCase();
		this.Schema=Schema;
	}
	
	
	/* public static void main(String[] args) {
		// TODO Auto-generated method stub
		String userCommand="DROP TABLE Students";
		String currentSchema="University";
		 DropTable dp= new DropTable(userCommand.substring("drop table".length()).trim(),currentSchema);
		 if(dp.findTable())
		 {
			 dp.storeTableContents();
			 System.out.println("Table Found for dropping");
			 dp.writeNewTableFile();
			 dp.storeColumnContents();
			 System.out.println("Columns Found for dropping");
			 dp.writeNewColumFile();
		 }
		 else
		 {
			 System.out.println("Table not found in the current schema");
		 }
	} */

	public boolean findTable()
	{
		boolean tableFound=false;
		try
		{
			RandomAccessFile SchTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
			
		while(true)
		{
			try
			{
			byte varcharLength = SchTableFile.readByte();
			
			String schemaName="";
			char schemaArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
			{
				schemaArr[i]=(char)SchTableFile.readByte();
			}
			schemaName=String.valueOf(schemaArr).trim();
			
			varcharLength = SchTableFile.readByte();
			
			String TableName="";
			char tableArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
			{
				tableArr[i]=(char)SchTableFile.readByte();
			}
			TableName=String.valueOf(tableArr).trim();
			
			long table_rows=SchTableFile.readLong();
			//once we read until schemata, we update the same
			if(TableName.equals(DrpTableName) && schemaName.equals(Schema) )
			{
				tableFound=true; //indicate that the table was found
				break;
			}
			
			}catch(EOFException e)
			{
				break;
			}
		}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return tableFound;
	}
	
	public void storeTableContents()
	{
		tableIndex = new TreeMap();
		try {
			
		RandomAccessFile StrTblFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
			
		while(true)
		{
			ArrayList<String> values=new ArrayList<>();
			try
			{
				byte varcharSchemaLength= StrTblFile.readByte();
				
				String schemaName="";
				char schemaArr[]=new char[30];
				for(int i = 0; i < varcharSchemaLength; i++)
				{
					schemaArr[i]=(char)StrTblFile.readByte();
				}
				schemaName=String.valueOf(schemaArr).trim();
					
				byte varcharTableLength = StrTblFile.readByte();
				
				String TableName="";
				char tableArr[]=new char[30];
				for(int i = 0; i < varcharTableLength; i++)
				{
					tableArr[i]=(char)StrTblFile.readByte();
				}
				TableName=String.valueOf(tableArr).trim();
					
				long table_rows=StrTblFile.readLong();
				
				if(!(TableName.equals(DrpTableName) && schemaName.equals(Schema) ))
				{
					 //indicate that the table was not the table to be dropped
					values.add(varcharSchemaLength+""); //0th element has schema length
					values.add(schemaName); //1st element has schema name
					values.add(varcharTableLength+""); //2nd element has schema name
					values.add(table_rows+""); //3rd element has schema name
					tableIndex.put(TableName, values);
				}
				
			}catch(EOFException e)
			{
				break;
			}
			
		}
			
		for(Entry<Object,ArrayList> entry : tableIndex.entrySet()) 
		{
			Object key = entry.getKey(); // Get the index key
			System.out.print(key + " => ");       // Display the index key
			ArrayList value = entry.getValue();   // Get the list of record addresses
			System.out.print("[" + value.get(0));// Display the first address
			
			for(int a=1; a < value.size();a++) {  // Check for and display additional addresses for non-unique indexes
			System.out.print("," + value.get(a));
				
			}
			System.out.println("]");
		} 
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	public void writeNewTableFile()
	{
		try 
		{
			boolean success = (new File("information_schema.tables.tbl")).delete();
			new FileOutputStream("information_schema.tables.tbl").close();
			System.out.println(success);
			
			RandomAccessFile SchTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
			int i=0;
			
				for(Entry<Object,ArrayList> entry : tableIndex.entrySet()) 
				{
				Object key = entry.getKey();
				ArrayList value = entry.getValue();
				byte schemaLength=Byte.parseByte(value.get(0)+"");
				SchTableFile.writeByte(schemaLength);
				SchTableFile.writeBytes(value.get(1)+"");
				byte tableLength=Byte.parseByte(value.get(2)+"");
				SchTableFile.writeByte(tableLength);
				SchTableFile.writeBytes((key)+"");
				SchTableFile.writeLong(Long.parseLong(value.get(3)+""));
				}
				
		
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void storeColumnContents()
	{
	columnIndex = new TreeMap();
	String schemaName="";
	byte varcharSchemaLength = 0;
	byte varcharTableLength=0;
	byte varcharColumnLength = 0;
	byte varcharColumnTypeLength=0;
	byte varcharNullValueLength=0;
	int ord_Pos=0;
	String TableName="";
	String columnName="";
	String columnType="";
	String nullCheck="";
	
	try {
		
	RandomAccessFile StrcolumFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
		
	while(true)
	{
		ArrayList<String> values=new ArrayList<>();
		boolean isPriKey=false;
		boolean nextRecord=false;
		try{
		
		varcharSchemaLength = StrcolumFile.readByte();
		schemaName="";
		char schemaArr[]=new char[30];
		for(int i = 0; i < varcharSchemaLength; i++)
			schemaArr[i]=(char)StrcolumFile.readByte();
		//reading SchemaName
		schemaName=String.valueOf(schemaArr).trim();
		
		
		
		
		varcharTableLength = StrcolumFile.readByte();
		TableName="";
		char tableArr[]=new char[30];
		for(int i = 0; i < varcharTableLength; i++)
			tableArr[i]=(char)StrcolumFile.readByte();
		//reading TableName
		TableName=String.valueOf(tableArr).trim();
		
		varcharColumnLength = StrcolumFile.readByte();
		columnName="";
		char columnArr[]=new char[30];
		for(int i = 0; i < varcharColumnLength; i++)
			columnArr[i]=(char)StrcolumFile.readByte();
		//reading columnName
		columnName=String.valueOf(columnArr).trim();
		
		
		
		ord_Pos=StrcolumFile.readInt(); //reading ordinal position
		
		//reading column Type
		varcharColumnTypeLength = StrcolumFile.readByte();
		
		columnType="";
		char columnTypeArr[]=new char[30];
		for(int i = 0; i < varcharColumnTypeLength; i++)
			columnTypeArr[i]=(char)StrcolumFile.readByte();
		
		columnType=String.valueOf(columnTypeArr).trim();
		
		
		//checking for Null condition
		
		varcharNullValueLength = StrcolumFile.readByte();
		
		nullCheck="";
		char nullCheckArr[]=new char[30];
		for(int i = 0; i < varcharNullValueLength; i++)
			nullCheckArr[i]=(char)StrcolumFile.readByte();
		
		nullCheck=String.valueOf(nullCheckArr).trim();
		
		long ptrLocation=StrcolumFile.getFilePointer();
		
		byte varcharAmbiLength = StrcolumFile.readByte();
		
		if(varcharAmbiLength==0) //next byte is 0, it marks the end of column
		{
			nextRecord=true;
		}
		
		else
		{
			//check for primary key
			StrcolumFile.seek(ptrLocation);
			varcharAmbiLength = StrcolumFile.readByte();
			String ambigiousName="";
			char ambigiousNameArr[]=new char[30];
			for(int i = 0; i < varcharAmbiLength; i++)
				ambigiousNameArr[i]=(char)StrcolumFile.readByte();
			
			ambigiousName=String.valueOf(ambigiousNameArr).trim();
			
			if(ambigiousName.equals("PRI"))
			{
				isPriKey=true;
			}
			else
			{
				StrcolumFile.seek(ptrLocation);
			}
		}
		
		
		
		if(!(TableName.equals(DrpTableName) && schemaName.equals(Schema)))
		{
			
			values.add(varcharSchemaLength+""); //0th element has schema length
			values.add(schemaName); //1st element has schema name
			values.add(varcharTableLength+""); //2nd element has schema name
			values.add(TableName);
			values.add(varcharColumnLength+"");
			values.add(columnName);
			values.add(ord_Pos+""); //3rd element has schema name
			values.add(varcharColumnTypeLength+"");
			values.add(columnType);
			values.add(varcharNullValueLength+"");
			values.add(nullCheck);
			if(isPriKey==true)
			{
				values.add(3+"");
				values.add("PRI");
			}
			columnIndex.put((schemaName+" "+TableName+" "+columnName), values);
		}
		
		else
		{
			columnList.add(columnName);
		}
		
		}catch(EOFException e) //break out of loop in case of EOF
		{
			
			if(!(TableName.equals(DrpTableName) && schemaName.equals(Schema)))
			{
				
				values.add(varcharSchemaLength+""); //0th element has schema length
				values.add(schemaName); //1st element has schema name
				values.add(varcharTableLength+""); //2nd element has schema name
				values.add(TableName);
				values.add(varcharColumnLength+"");
				values.add(columnName);
				values.add(ord_Pos+""); //3rd element has schema name
				values.add(varcharColumnTypeLength+"");
				values.add(columnType);
				values.add(varcharNullValueLength+"");
				values.add(nullCheck);
				if(isPriKey==true)
				{
					values.add(3+"");
					values.add("PRI");
				}
				columnIndex.put((schemaName+" "+TableName+" "+columnName), values);
			}
			else
			{
				columnList.add(columnName);
			}
			break;
			
		}			
	}
		for(Entry<Object,ArrayList> entry : columnIndex.entrySet()) 
		{
			Object key = entry.getKey(); // Get the index key
			System.out.print(key + " => ");       // Display the index key
			ArrayList value = entry.getValue();   // Get the list of record addresses
			System.out.print("[" + value.get(0));// Display the first address
		
			for(int a=1; a < value.size();a++) {  // Check for and display additional addresses for non-unique indexes
				System.out.print("," + value.get(a));
			
			}
			System.out.println("]");
		} 
	
	
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void writeNewColumFile()
	{
		try 
		{
			boolean success = (new File("information_schema.columns.tbl")).delete();
			new FileOutputStream("information_schema.columns.tbl").close();
			
			RandomAccessFile SchColumnFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			int i=0;
			
				for(Entry<Object,ArrayList> entry : columnIndex.entrySet()) 
				{
				Object key = entry.getKey();
				ArrayList value = entry.getValue();
				byte schemaLength=Byte.parseByte(value.get(0)+"");
				SchColumnFile.writeByte(schemaLength);
				SchColumnFile.writeBytes(value.get(1)+"");
				byte tableLength=Byte.parseByte(value.get(2)+"");
				SchColumnFile.writeByte(tableLength);
				SchColumnFile.writeBytes(value.get(3)+"");
				byte columnLength=Byte.parseByte(value.get(4)+"");
				SchColumnFile.writeByte(columnLength);
				SchColumnFile.writeBytes(value.get(5)+"");
				SchColumnFile.writeInt(Integer.parseInt(value.get(6)+""));
				byte columnTypeLength=Byte.parseByte(value.get(7)+"");
				SchColumnFile.writeByte(columnTypeLength);
				SchColumnFile.writeBytes(value.get(8)+"");
				byte isNullLength=Byte.parseByte(value.get(9)+"");
				SchColumnFile.writeByte(isNullLength);
				SchColumnFile.writeBytes(value.get(10)+"");
				
				if(value.size()>11) //printing in case of primary key
				{
					SchColumnFile.writeBytes(value.get(11)+"");//length of PRI
					SchColumnFile.writeBytes(value.get(12)+"");
				}
				}
				
		
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void deleteFiles()
	{
		try
		{
		new FileOutputStream((Schema+"."+DrpTableName+".tbl")).close();
		boolean success=(new File(Schema+"."+DrpTableName+".tbl")).delete();
		//System.out.println(success);
		for(String column:columnList)
		{
			new FileOutputStream((Schema+"."+DrpTableName+"."+column+".ndx")).close();
			(new File(Schema+"."+DrpTableName+"."+column+".ndx")).delete();
		}
		
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
}
