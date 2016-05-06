import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


public class InsertTable {

	String insTableName;
	ArrayList<String> columnList =new ArrayList<>();
	ArrayList<String> columnTypes= new ArrayList<>();
	ArrayList<String> isNullable = new ArrayList<>();
	ArrayList<String> primaryKeyValues = new ArrayList<>();
	String[] insertedValues;
	String Schema;
	String primaryKey="";
	String primaryKeyType;
	int primaryKeyPos;
	
	InsertTable(String tableInsertStmt, String Schema)
	{
		this.Schema=Schema;
		insTableName=tableInsertStmt.substring(0,tableInsertStmt.indexOf(" VALUES")).trim().toLowerCase();
		insertedValues=tableInsertStmt.substring(tableInsertStmt.indexOf("(")+1).split(",");
		int collength=insertedValues.length;
		insertedValues[collength-1]=insertedValues[collength-1].substring(0,insertedValues[collength-1].length()-1);
		/*for(String t:insertedValues)
		{
			//System.out.println(t);
		} */
		for(int ind=0;ind<insertedValues.length;ind++)
		{
			insertedValues[ind]=insertedValues[ind].trim(); //trimming all the values
			insertedValues[ind]=insertedValues[ind].replace("'", "");
		}
	}
	
	
 /*	public static void main(String[] args)
	{
		String sqlst="insert into firstTable values(4,'Anirudh',8)";
		InsertTable it=new InsertTable(sqlst.substring("insert into".length()).trim(),"first");
		it.gettingColumns();
		it.insertIntoTbl();
	} */
	
	public boolean gettingColumns()
	{
		boolean tableFound=false;
		boolean isSchemaName=false;
		byte varcharLength;
		String schemaName="";
		String TableName="";
		int index=0;
		RandomAccessFile InfoTableFile=null;
		try
		{
			InfoTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
		
		while(true)
		{
			try{
			if(isSchemaName==false)
			{
			varcharLength = InfoTableFile.readByte();
			schemaName="";
			char schemaArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
				schemaArr[i]=(char)InfoTableFile.readByte();
			//reading SchemaName
			schemaName=String.valueOf(schemaArr).trim();
			}
			
			isSchemaName=false; //make the isSchemaName as false for next
			
			varcharLength = InfoTableFile.readByte();
			TableName="";
			char tableArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
				tableArr[i]=(char)InfoTableFile.readByte();
			//reading TableName
			TableName=String.valueOf(tableArr).trim();
			
			varcharLength = InfoTableFile.readByte();
			String columnName="";
			char columnArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
				columnArr[i]=(char)InfoTableFile.readByte();
			//reading columnName
			columnName=String.valueOf(columnArr).trim();
			
			if(TableName.equals(insTableName) && schemaName.equals(Schema))
			{
				columnList.add(columnName);
				tableFound=true;
				//System.out.println(); //for differentiating
				//System.out.print("column: "+columnName);
			}
			
			int ord_Pos=InfoTableFile.readInt(); //reading ordinal position
			varcharLength = InfoTableFile.readByte();
			
			String columnType="";
			char columnTypeArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
				columnTypeArr[i]=(char)InfoTableFile.readByte();
			
			columnType=String.valueOf(columnTypeArr).trim();
			
			if(TableName.equals(insTableName) && schemaName.equals(Schema))
			{
				columnTypes.add(columnType);//adding column type
				//System.out.print(" column type: "+columnType);
			}
			
			varcharLength = InfoTableFile.readByte();
			
			String nullCheck="";
			char nullCheckArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
				nullCheckArr[i]=(char)InfoTableFile.readByte();
			
			nullCheck=String.valueOf(nullCheckArr).trim();
			
			if(TableName.equals(insTableName) && schemaName.equals(Schema))
			{
				isNullable.add(nullCheck);//adding column type
				//System.out.print(" is Null: "+nullCheck);
			}
			
			varcharLength = InfoTableFile.readByte();
			
			if(varcharLength==0) //next byte is 0, it marks the end of column
			{
				continue;
			}
			
			String ambigiousName="";
			char ambigiousNameArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
				ambigiousNameArr[i]=(char)InfoTableFile.readByte();
			
			ambigiousName=String.valueOf(ambigiousNameArr).trim();
			
			if(TableName.equals(insTableName) && schemaName.equals(Schema))
			{
				index++;
			}
			
			if(ambigiousName.equals("PRI"))
			{
				if(TableName.equals(insTableName) && schemaName.equals(Schema))
				{
					primaryKey=columnName;
					primaryKeyType=columnType;
					primaryKeyPos=index-1;
					//System.out.print(" is Primary");
				}
			}
			else
			{
				schemaName=ambigiousName;
				isSchemaName=true;
			}
			
			
			}catch(EOFException e) //break out of loop in case of EOF
			{
				break;
			}
			
			
		}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try {
				InfoTableFile.close();
			} catch (IOException e) {
				
			}
		}
		
		return tableFound;
	}
	
	public boolean gettingCount()
	{
		boolean mismatch=false;
		if(insertedValues.length!=columnList.size())
		{
			mismatch=true;
		}
		
		return mismatch;
	}
	
	
	public boolean insertIntoTbl()
	{
		boolean noNullValueError=true;
		RandomAccessFile tblFile=null;
		RandomAccessFile columnFile=null;
		try {
			tblFile= new RandomAccessFile((Schema+"."+insTableName+".tbl"), "rw");
			
			
			for(int i=0;i<insertedValues.length;i++)
			{
			if(isNullable.get(i).equals("NO") && (insertedValues[i].equals("") || insertedValues[i].equals("null") || insertedValues[i].equals("NULL")))
			{
				
				noNullValueError=false;
				return noNullValueError;
			}
			}
			
			
			while(true) //looping to reach EOF of .tbl File
			{
			try{
				
				tblFile.readByte();
				
			}catch(EOFException e)
			{
				break;
			}
			}
			
			long tblLocation=tblFile.getFilePointer();
			
		for(int i=0;i<insertedValues.length;i++)
		{
			columnFile= new RandomAccessFile((Schema+"."+insTableName+"."+columnList.get(i)+".ndx"), "rw");
			
			while(true) //looping to reach EOF of .ndx File
			{
			try{
				
				columnFile.readByte();
				
			}catch(EOFException e)
			{
				break;
			}
			}
			
			
			switch(columnTypes.get(i))
			{
			
			case "int":
			case "INT":
				if(insertedValues[i].equals("NULL"))
				{
					insertedValues[i]="0";
				}
				columnFile.writeInt(Integer.parseInt(insertedValues[i]));
				columnFile.writeInt(1);
				columnFile.writeLong(tblLocation);
				tblFile.writeInt(Integer.parseInt(insertedValues[i]));
				break;
				
			case "long":
			case "LONG":
				if(insertedValues[i].equals("NULL"))
				{
					insertedValues[i]="0";
				}
				columnFile.writeLong(Long.parseLong(insertedValues[i]));
				columnFile.writeInt(1);
				columnFile.writeLong(tblLocation);
				tblFile.writeLong(Long.parseLong(insertedValues[i]));
				break;
				
			case "short":
			case "SHORT":
				if(insertedValues[i].equals("NULL"))
				{
					insertedValues[i]="0";
				}
				columnFile.writeShort(Short.parseShort(insertedValues[i]));
				columnFile.writeInt(1);
				columnFile.writeLong(tblLocation);
				tblFile.writeShort(Short.parseShort(insertedValues[i]));
				break;
				
			case "float":
			case "FLOAT":
				if(insertedValues[i].equals("NULL"))
				{
					insertedValues[i]="0";
				}
				columnFile.writeFloat(Float.parseFloat(insertedValues[i]));
				columnFile.writeInt(1);
				columnFile.writeLong(tblLocation);
				tblFile.writeFloat(Float.parseFloat(insertedValues[i]));
				break;
				
			case "double":
			case "DOUBLE":
				if(insertedValues[i].equals("NULL"))
				{
					insertedValues[i]="0";
				}
				columnFile.writeDouble(Double.parseDouble(insertedValues[i]));
				columnFile.writeInt(1);
				columnFile.writeLong(tblLocation);
				tblFile.writeDouble(Double.parseDouble(insertedValues[i]));
				break;
				
			case "byte":
			case "BYTE":
				if(insertedValues[i].equals("NULL"))
				{
					insertedValues[i]="0";
				}
				columnFile.writeByte(Byte.parseByte(insertedValues[i]));
				columnFile.writeInt(1);
				columnFile.writeLong(tblLocation);
				tblFile.writeByte(Byte.parseByte(insertedValues[i]));
				break;
				
			case "date":
			case "DATE":
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				insertedValues[i]=insertedValues[i].replace("'","");
				long dateInLong=0;
				if(insertedValues[i].equals("NULL"))
				{
					insertedValues[i]="0";
					
				}
				else
				{
			    Date date = formatter.parse(insertedValues[i]);
			    dateInLong = date.getTime();
				}
			    
			    
			    columnFile.writeLong(dateInLong);
				columnFile.writeInt(1);
				columnFile.writeLong(tblLocation);
				tblFile.writeLong(dateInLong);
				break;
			
			case "datetime":
			case "DATETIME":
				if(insertedValues[i].equals("NULL"))
				{
					insertedValues[i]="0";
				}
				DateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				insertedValues[i]=insertedValues[i].replace("'","");
			    Date datetimevar = formatter2.parse(insertedValues[i]);
			    long dateInLongvar = datetimevar.getTime();
			    
			    columnFile.writeLong(dateInLongvar);
				columnFile.writeInt(1);
				columnFile.writeLong(tblLocation);
				tblFile.writeLong(dateInLongvar);
				break;
				
				
			default:				
				
				if(columnTypes.get(i).contains("varchar") || columnTypes.get(i).contains("char") || columnTypes.get(i).contains("VARCHAR") || columnTypes.get(i).contains("CHAR"))
				{
					int len=0;
					String len1="";
					
					insertedValues[i]=insertedValues[i].replace("'","");
					
					len1=(columnTypes.get(i).substring(columnTypes.get(i).indexOf("(")+1,columnTypes.get(i).lastIndexOf(")")));
					len= Integer.parseInt(len1);
					
					tblFile.writeByte(insertedValues[i].length());
					columnFile.writeByte(insertedValues[i].length());
					
					for(int j=0;j<len && j<insertedValues[i].length();j++)
					{
						columnFile.writeByte(insertedValues[i].charAt(j)); //writing only until length in column file
					}
					
					columnFile.writeInt(1);
					columnFile.writeLong(tblLocation);
					
					for(int j=0;j<len && j<insertedValues[i].length();j++)
					{
						tblFile.writeByte(insertedValues[i].charAt(j)); //writing only until length in table file
					}
					
				}
				break;
			}
			
		}
		
		updateinfoTables(); //updating info_schma.tables file for row count
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		finally
		{
			try
			{
			tblFile.close();
			columnFile.close();
			}catch(Exception e)
			{
				
			}
		}
		
		return noNullValueError;
		
	}
		
	public void updateinfoTables()
	{
		RandomAccessFile SchTableFile=null;
		RandomAccessFile SchTableFile2=null;
		try
		{
			SchTableFile = new RandomAccessFile("information_schema.tables.tbl", "rw");
			SchTableFile2 = new RandomAccessFile("information_schema.tables.tbl", "rw");
		while(true)
		{
			byte varcharLength = SchTableFile.readByte();
			SchTableFile2.readByte();
			
			String schemaName="";
			char schemaArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
			{
				schemaArr[i]=(char)SchTableFile.readByte();
				SchTableFile2.readByte();
			}
			schemaName=String.valueOf(schemaArr).trim();
			
			varcharLength = SchTableFile.readByte();
			SchTableFile2.readByte();
			
			String TableName="";
			char tableArr[]=new char[30];
			for(int i = 0; i < varcharLength; i++)
			{
				tableArr[i]=(char)SchTableFile.readByte();
				SchTableFile2.readByte();
			}
			TableName=String.valueOf(tableArr).trim();
			
			Long table_rows=SchTableFile.readLong();
			//once we read until schemata, we update the same
			if(TableName.equals(insTableName) && schemaName.equals(Schema) )
			{
				SchTableFile2.writeLong(++table_rows);
				break;
			}
			else
			{
				SchTableFile2.readLong();
			}
			
		}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
			SchTableFile.close();
			SchTableFile2.close();
			}catch(IOException e)
			{
				
			}
		}
	}
	
	public boolean checkPrimaryKey()
	{
		boolean primaryKeyViolated=false;
		
		if(primaryKey.equals("")) //in case of no primary Key, return false
		{
			return primaryKeyViolated;
		}
		
		if(insertedValues[primaryKeyPos].equals("NULL"))
		{
			return primaryKeyViolated;
		}
		
		
		RandomAccessFile priKeyFile=null;
		try {
			priKeyFile= new RandomAccessFile((Schema+"."+insTableName+"."+primaryKey+".ndx"), "rw");
			
			while(true)
			{
				try
				{
					switch(primaryKeyType)
					{
					case "int":
					if(Integer.parseInt(insertedValues[primaryKeyPos])==priKeyFile.readInt())
						primaryKeyViolated=true;
					
					priKeyFile.readInt();
					priKeyFile.readLong();
						break;
						
					case "long":
					case "long int":
						if(Long.parseLong(insertedValues[primaryKeyPos])==priKeyFile.readLong())
							primaryKeyViolated=true;
						
						priKeyFile.readInt();
						priKeyFile.readLong();
						break;
						
					case "short":
					case "short int":
						if(Short.parseShort(insertedValues[primaryKeyPos])==priKeyFile.readShort())
							primaryKeyViolated=true;
						
						priKeyFile.readInt();
						priKeyFile.readLong();
						break;
						
						
					case "byte":
						if(Byte.parseByte(insertedValues[primaryKeyPos])==priKeyFile.readByte())
							primaryKeyViolated=true;
						
						priKeyFile.readInt();
						priKeyFile.readLong();
						break;
						
					case "float":
						if(Float.parseFloat(insertedValues[primaryKeyPos])==priKeyFile.readFloat())
							primaryKeyViolated=true;
						
						priKeyFile.readInt();
						priKeyFile.readLong();
						break;
					
					case "double":
						if(Double.parseDouble(insertedValues[primaryKeyPos])==priKeyFile.readDouble())
							primaryKeyViolated=true;
						
						priKeyFile.readInt();
						priKeyFile.readLong();
						break;
						
					case "date" :
						if(Long.parseLong(insertedValues[primaryKeyPos])==priKeyFile.readLong())
							primaryKeyViolated=true;
						
						priKeyFile.readInt();
						priKeyFile.readLong();
						break;
						
					default:
						if(primaryKeyType.contains("varchar") || primaryKeyType.contains("char") || primaryKeyType.contains("VARCHAR") || primaryKeyType.contains("CHAR"))
						{
							insertedValues[primaryKeyPos]=insertedValues[primaryKeyPos].replace("'","");
							byte varcharLength=priKeyFile.readByte();
							String keyChar="";
							char columnValArr[]=new char[30];
							for(int x = 0; x < varcharLength; x++)
								columnValArr[x]=(char)priKeyFile.readByte();
							//reading Key
							keyChar=String.valueOf(columnValArr).trim();	
							
							if(keyChar.equals(insertedValues[primaryKeyPos]))
							{
								primaryKeyViolated=true;
							}
							priKeyFile.readInt();
							priKeyFile.readLong();
						}
						
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
		
		finally
		{
			try {
				priKeyFile.close();
			} catch (IOException e) {
			
			}
		}
		
		return primaryKeyViolated;
	}   
	
	
}
	
