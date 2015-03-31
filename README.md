# PBIAPIStarter
This is a small tool to create, update datasets on your Power BI account using data from SQL Server database

This tool uses Power BI API to connect to your Power BI site.
 
The PBIStarter console application allows you to
            Create a dataset on your Power BI site based on a SQL Server database table
            Refresh existing dataset
            See list of your datasets             
            Iterative data push in case of huge data

The tool currently
	Is limited to SQL Server Data Source only
	Type casts to relevant datatypes in order to retain the data type since power BI supports only certain types.
		E.g. Date is converted to DateTime. Float is converted to Double...etc.
	Tool synchronously sends database table data until all the rows are updated to Power BI dataset.
	Allows one table per dataset
	    
For more details about Power BI API https://msdn.microsoft.com/en-US/library/dn889824.aspx#AAD
