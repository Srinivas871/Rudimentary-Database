import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;
import java.util.Map.Entry;


public class SelectQuery {
	
	String selTableName;
	ArrayList<String> columnList =new ArrayList<>();
	ArrayList<String> columnTypes= new ArrayList<>();
	ArrayList<String> isNullable = new ArrayList<>();
	ArrayList<Object> keyArr = new ArrayList<>();
	ArrayList<Object> valuesArr = new ArrayList<>();
	ArrayList<String> operatorsUsed = new ArrayList<>();
	String Schema;
	String primaryKey="";
	String whereColumn;
	int whereColumnIndex;
	String whereValue;
	boolean whereSpecified=false;
	TreeMap<ArrayList<Long>, Object> columnIndex;

	public SelectQuery(String tableSelectStmt, String Schema)
	{
		String firstOprtr="",lastOprtr="";
		int oprtrcnt=0;
		this.Schema=Schema;
		whereSpecified=true;
		String[] allowedOperators={">","<","!","="};
		if(tableSelectStmt.contains("WHERE"))
		{
			for(String s: allowedOperators)
			{
				if(tableSelectStmt.contains(s))
				{
					operatorsUsed.add(s);
					oprtrcnt++;
					if(oprtrcnt==1)
					{
						firstOprtr=s;
						lastOprtr=s;
					}
				}
			}
			
			if(oprtrcnt>1)
				lastOprtr=operatorsUsed.get(operatorsUsed.size()-1);
			
		selTableName=tableSelectStmt.substring(0,tableSelectStmt.indexOf(" WHERE")).trim().toLowerCase();
		whereColumn=tableSelectStmt.substring(tableSelectStmt.lastIndexOf(" WHERE")+" WHERE".length(),tableSelectStmt.indexOf(firstOprtr)).trim().toLowerCase();
		whereValue=tableSelectStmt.substring(tableSelectStmt.lastIndexOf(firstOprtr)+1);
		whereValue=whereValue.replace("'","");
		//System.out.println(whereColumn);
		//System.out.println(whereValue);
		
		}
		else
		{
			whereSpecified=false;
			selTableName=tableSelectStmt.trim().toLowerCase();
		}
	}
	

	/* public static void main(String[] args) {
		// TODO Auto-generated method stub
		//String sqlst="SELECT * FROM Students";
		String sqlst="SELECT * FROM Students WHERE id>1";
		SelectQuery sq=new SelectQuery(sqlst.substring("select * from".length()).trim(),"University");
		sq.gettingColumns();
		sq.findInitialAddress();
		sq.display(); 
	} */
	
	public boolean gettingColumns()
	{
		boolean tableFound=false;
		boolean isSchemaName=false;
		byte varcharLength;
		String schemaName="";
		
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
			String TableName="";
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
			
			if(TableName.equals(selTableName) && schemaName.equals(Schema))
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
			
			if(TableName.equals(selTableName) && schemaName.equals(Schema))
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
			
			if(TableName.equals(selTableName) && schemaName.equals(Schema))
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
			
			if(ambigiousName.equals("PRI"))
			{
				if(TableName.equals(selTableName) && schemaName.equals(Schema))
				{
					primaryKey=columnName;
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
	
	public void findInitialAddress()
	{ 
		RandomAccessFile IndxFile=null,PrimaryIndxFile=null;
		try
		{
			
			columnIndex = new TreeMap();
			int i=0;
			int no_of_values=0;
			
			if(whereSpecified==false)
			{
				whereColumn=primaryKey;
			}
			
			if(primaryKey.equals("") && whereSpecified==false)
			{
				whereColumn=columnList.get(0);
			}
			
			IndxFile = new RandomAccessFile((Schema+"."+selTableName+"."+whereColumn+".ndx"), "rw");
			PrimaryIndxFile= new RandomAccessFile((Schema+"."+selTableName+"."+primaryKey+".ndx"), "rw");
			
			for(String column:columnList)
			{
				if(column.equals(whereColumn))
					break;
				else
					i++;
			}
			
			
			if(primaryKey.equals(""))
			{
				i=0;
			}
			
			whereColumnIndex=i;//storing the where column no
			
			while(true)
			{	
				ArrayList<Long> values=new ArrayList<>();
				try
				{
				switch(columnTypes.get(i))
				{
				
				case "int":
				case "INT":
					Integer keyInt=IndxFile.readInt();
					no_of_values=IndxFile.readInt();
					for(int x=0;x<no_of_values;x++)
					{
						values.add(IndxFile.readLong()); //adding addresses
					}
					if(whereSpecified==false || checkCondn(keyInt+"","int"))
					{
						valuesArr.add(values.get(0));
					}
					break;
					
				case "long":
				case "LONG":
					Long keyLong=IndxFile.readLong();
					no_of_values=IndxFile.readInt();
					for(int x=0;x<no_of_values;x++)
					{
						values.add(IndxFile.readLong()); //adding addresses
					}
					if(whereSpecified==false || checkCondn(keyLong+"","long"))
						valuesArr.add(values.get(0));
					break;
					
				case "short":
				case "SHORT":
					Short keyShort=IndxFile.readShort();
					no_of_values=IndxFile.readInt();
					for(int x=0;x<no_of_values;x++)
					{
						values.add(IndxFile.readLong()); //adding addresses
					}
					if(whereSpecified==false || checkCondn(keyShort+"","short"))
						valuesArr.add(values.get(0));
					break;
					
				case "float":
				case "FLOAT":
					Float keyFloat=IndxFile.readFloat();
					no_of_values=IndxFile.readInt();
					for(int x=0;x<no_of_values;x++)
					{
						values.add(IndxFile.readLong()); //adding addresses
					}
					if(whereSpecified==false || checkCondn(keyFloat+"","float"))
						valuesArr.add(values.get(0));
					break;
					
				case "double":
				case "DOUBLE":
					Double keyDouble=IndxFile.readDouble();
					no_of_values=IndxFile.readInt(); 
					for(int x=0;x<no_of_values;x++)
					{
						values.add(IndxFile.readLong()); //adding addresses
					}
					if(whereSpecified==false || checkCondn(keyDouble+"","double"))
						valuesArr.add(values.get(0));
					break;
					
				case "byte":
				case "BYTE":
					Byte keyByte=IndxFile.readByte();
					no_of_values=IndxFile.readInt(); 
					
					for(int x=0;x<no_of_values;x++)
					{
						values.add(IndxFile.readLong()); //adding addresses
					}
					if(whereSpecified==false || checkCondn(keyByte+"","byte"))
						valuesArr.add(values.get(0));
					break;
					
				case "date":
				case "DATE":
					DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				    long dateInLong = IndxFile.readLong();
				    Date keyDate=new Date(dateInLong);
				    no_of_values=IndxFile.readInt();
					for(int x=0;x<no_of_values;x++)
					{
						values.add(IndxFile.readLong());
					}
					if(whereSpecified==false || checkCondn(formatter.format(keyDate)+"","date"))
						{
						valuesArr.add(values.get(0));
						}
					
					break;
				
				case "datetime":
				case "DATETIME":
					DateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				    long dateInLongvar = IndxFile.readLong();
				    Date keyDateTime=new Date(dateInLongvar);
				    no_of_values=IndxFile.readInt();
				    for(int x=0;x<no_of_values;x++)
					{
				    	values.add(IndxFile.readLong());
					}
				    if(whereSpecified==false || checkCondn(formatter2.format(keyDateTime)+"","date"))
						{
				    	valuesArr.add(values.get(0));
						}
					
					break;
					
					
				default:				
					
					if(columnTypes.get(i).contains("varchar") || columnTypes.get(i).contains("char") || columnTypes.get(i).contains("VARCHAR") || columnTypes.get(i).contains("CHAR"))
					{
					byte varcharLength=IndxFile.readByte();
					String keyChar="";
					char columnValArr[]=new char[30];
					for(int x = 0; x < varcharLength; x++)
						columnValArr[x]=(char)IndxFile.readByte();
					//reading Key
					keyChar=String.valueOf(columnValArr).trim();
					
					
					no_of_values=IndxFile.readInt();
					
					for(int x=0;x<no_of_values;x++)
					{
						values.add(IndxFile.readLong());
					}
					if(whereSpecified==false || checkCondn(keyChar,"char"))
						valuesArr.add(values.get(0));
					}
					break;
				}
				
			}catch(EOFException e)
			{
				break;
			}
		}
		/*	for(Entry<ArrayList<Long>,Object> entry : columnIndex.entrySet()) 
			{
				/*Object key = entry.getKey(); // Get the index key
				//System.out.print(key + " => ");       // Display the index key
				ArrayList value = entry.getValue();   // Get the list of record addresses
				//System.out.print("[" + value.get(0));// Display the first address
				valuesArr.add(value.get(0));
				for(int a=1; a < entry.getKey().size();a++) {  // Check for and display additional addresses for non-unique indexes
					//System.out.print("," + value.get(a));
					valuesArr.add(entry.getKey().get(a));
				} 
				//System.out.println("]");
			} */
			
		}catch(Exception e)
		{
		 e.printStackTrace();	
		}
		
		finally
		{
			try {
				IndxFile.close();
			} catch (IOException e) {
				
			}
		}
		
	}
	
	public void display()
	{
		int count=0,i=0;
		long indexFileLocation;
		RandomAccessFile TblFile=null;
		try
		{
		TblFile = new RandomAccessFile((Schema+"."+selTableName+".tbl"), "rw");
		
		for(String colHead:columnList)
			System.out.print(colHead+"\t");
		
		System.out.println();
		while(i<valuesArr.size())
		{
			indexFileLocation= (long) valuesArr.get(i);
			TblFile.seek(indexFileLocation);
		for(String ctype:columnTypes)
		{
		long value=0;
		double floatValue=0;
		try{
			switch(ctype)
			{
			case "int":
			case "INT":
				value=TblFile.readInt();
				System.out.print(value+"  ");
				break;
				
			case "long":
			case "LONG":
				value=TblFile.readLong();
				System.out.print(TblFile.readLong()+"  ");
				break;
				
			case "float":
			case "FLOAT":
				floatValue=TblFile.readFloat();
				System.out.print(floatValue+"  ");
				break;
				
			case "short":
			case "SHORT":
				value=TblFile.readShort();
				System.out.print(value+"  ");
				break;
				
			case "double":
			case "DOUBLE":
				floatValue=TblFile.readDouble();
				System.out.print(floatValue+"  ");
				break;
				
			case "byte":
			case "BYTE":
				value=TblFile.readByte();
				System.out.print(value+"  ");
				break;
				
			case "date":
			case "DATE":
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			    long dateInLong = TblFile.readLong();
			    Date dt1=new Date(dateInLong);
			    if(dateInLong==0)
			    {
			    	System.out.print("NULL"+"  ");
			    }
			    else
			    	System.out.print(formatter.format(dt1)+"  ");
			    break;
			    
			case "datetime":
			case "DATETIME":
				DateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
			    long dateInLong2 = TblFile.readLong();
			    Date dt2=new Date(dateInLong2);
			    
			    if(dateInLong2==0)
			    {
			    	System.out.print("NULL"+"  ");
			    }
			    else
			    	System.out.print(formatter2.format(dt2)+"  ");
			    
			    break;
			    
			default:
			    if(ctype.contains("varchar") || ctype.contains("char") || ctype.contains("VARCHAR") || ctype.contains("CHAR"))
				{
				byte varcharLength=TblFile.readByte();
				String keyChar="";
				char columnValArr[]=new char[30];
				for(int x = 0; x < varcharLength; x++)
					columnValArr[x]=(char)TblFile.readByte();
				//reading Key
				keyChar=String.valueOf(columnValArr).trim();
				System.out.print(keyChar+"  ");
				}
			   break; 
			
			}
			}catch(EOFException ex)
			{
				break;
			}
		  }
			System.out.println();
			i++;
		 }
		}catch(Exception e)
		{
			
		}
		finally
		{
			try {
				TblFile.close();
			} catch (IOException e) {
				
			}
		}
	}
	
	public boolean checkCondn(String ob1, String type)
	{
		boolean checkcndn=false;
		int count=0;
		
		switch(type)
		{
		
		case "int":
		case "short":
		case "byte":
		case "long":
		long left1=Long.parseLong(ob1);
		long right1=Long.parseLong(whereValue);
		
		for(String op: operatorsUsed)
		{
			if(op.equals("=")&&left1==right1)
				count++;
				
			if(op.equals(">")&&left1>right1)
				count++;
			if(op.equals("<")&&left1<right1)
				count++;
		}
		
		break;
		
		case "char":
		case "date":
			String left2 = ob1;
			String right2 =whereValue;
		
			for(String op: operatorsUsed)
			{
				if(op.equals("=")&&left2.equals(right2))
					count++;
					
				if(op.equals(">")&&left2.compareTo(right2)>0)
					count++;
				if(op.equals("<")&&left2.compareTo(right2)<0)
					count++;
			}
			
		break;
		
		case "float":
		case "double":
			double left3 = Double.parseDouble(ob1);
			double right3 = Double.parseDouble(whereValue);
		
			for(String op: operatorsUsed)
			{
				if(op.equals("=")&&left3==right3)
					count++;
					
				if(op.equals(">")&&left3>right3)
					count++;
				if(op.equals("<")&&left3<right3)
					count++;
			}
			
		break;
		
		}
		
		if(count==operatorsUsed.size())
			checkcndn=true;
		
		return checkcndn;
	}
	
	
}
