package sailpoint.mcspoofing.task;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Calendar;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Stack;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.*;
import java.io.*;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import sailpoint.api.SailPointContext;
import sailpoint.api.IncrementalObjectIterator;
import sailpoint.api.IdentityService;
import sailpoint.object.Application;
import sailpoint.object.Attributes;
import sailpoint.object.AuditConfig;
import sailpoint.object.AuditConfig.AuditAction;
import sailpoint.object.AuditConfig.AuditAttribute;
import sailpoint.object.AuditConfig.AuditClass;
import sailpoint.object.Bundle;
import sailpoint.object.ClassLists;
import sailpoint.object.Configuration;
import sailpoint.object.Custom;
import sailpoint.object.Dictionary;
import sailpoint.object.DictionaryTerm;
import sailpoint.object.Filter;
import sailpoint.object.Identity;
import sailpoint.object.Link;
import sailpoint.object.ObjectAttribute;
import sailpoint.object.ObjectConfig;
import sailpoint.object.Profile;
import sailpoint.object.QueryOptions;
import sailpoint.object.SailPointObject;
import sailpoint.object.TaskDefinition;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskResult.CompletionStatus;
import sailpoint.object.TaskSchedule;
import sailpoint.object.UIConfig;
import sailpoint.server.Exporter;
import sailpoint.server.Exporter.Cleaner;
import sailpoint.task.BasePluginTaskExecutor;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;
import sailpoint.tools.xml.AbstractXmlObject;
import sailpoint.tools.xml.XMLObjectFactory;
import sailpoint.tools.xml.XMLReferenceResolver;
import sailpoint.tools.Message;
import sailpoint.tools.Message.Type;
import sailpoint.mcspoofing.common.CommonMethods;
// cannot import javax.mail.Message;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.InternetAddress;
import javax.mail.BodyPart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.Session;
import javax.mail.internet.MimeMultipart;
import javax.mail.Multipart;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.activation.DataHandler;
import javax.mail.internet.MimePartDataSource;
import javax.mail.Transport;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.util.*;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;

/**
 * Compare account data for multiple accounts on an identity
 *
 * @author Keith Smith
 */
public class CompareAccountData extends BasePluginTaskExecutor {
	private static final String PLUGIN_NAME = "MCSpoofingPlugin";
	
	private static Log log = LogFactory.getLog(CompareAccountData.class);
	private boolean terminate=false;
	private boolean unixOrigin=false;
	private int totalCount=0;
	private long oldestDate=0L;
	private StringBuffer sboutput=new StringBuffer();
	private String appName="";
	private String fieldName="";
	private String searchValue="";
	private String compareField=null;
	private boolean ignoreCase=true;
	private boolean ignoreNullsOnCompare=false;
	private boolean genHistogram=false;
	private boolean checkCorr=true;
	private boolean genIdentityList=false;
	private String multiSearch="";
	private List<String> applicationNames=null;
	private List<Application> applications=new ArrayList<Application>();
	private Boolean onlyActiveIdentities=true;
	private Boolean onlyCorrelatedIdentities=true;
	private String compareMapNameStr="";
	private Map<String,Object> compareMapOrig=new HashMap<String,Object>();
	private SortedMap<String,Object> compareMapObj=new TreeMap<String,Object>();
	private Boolean anyOfTheseFilter=false;
	private List<Object> identities=new ArrayList<Object>();
	private Boolean getAllIdentities=false;
	private List<String> processIdentities = new ArrayList<String>();
	private String resultsOutputStr="";
	private String outputFilenameStr="";
	private boolean sendToEmail=false;
	private boolean successfulFileWrite=false;
	private String outputFolderpath="";
	private String outputFilename="";
	private Boolean showAllData=false;
	private String filterString=null;
	private String orderbyString=null;
	
	private TaskResult taskResult;
	@SuppressWarnings({"rawtypes","unchecked"})
	@Override
	public void execute(SailPointContext context, TaskSchedule schedule,
		TaskResult result, Attributes args) throws Exception {
		DateFormat sdfout=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		DateFormat sdfnotime=new SimpleDateFormat("MM/dd/yyyy");
		Date now=new Date();
		Date nowNoTime=now;
		long nowTimeLong=now.getTime();
		Long nowTimeLongObj=Long.valueOf(nowTimeLong);
		String nowTimeLongStr=nowTimeLongObj.toString().trim();
		String nowDateNoTimeStr=sdfnotime.format(now);
		IdentityService idService=new IdentityService(context);
		try {
			nowNoTime=sdfnotime.parse(nowDateNoTimeStr);
		}
		catch (Exception et) {}
		oldestDate=nowNoTime.getTime();
		sboutput.append("CompareAccountData started at "+sdfout.format(now));
		log.debug("CAD-001 Starting Search Account Data");
		taskResult=result;
		/*
		 * Gather inputs starting with applications
		 * It is pretty much required to have multiple applications
		 */
		try {
			applicationNames=new ArrayList<String>();
			appName=args.getString("applications");
			log.debug("CAD-002 Application names:"+appName);
			if(appName.contains(",")) {
				String[] appArray = appName.split(",");
				log.debug("CAD-059 appArray length = "+appArray.length);
				for (int iapp=0; iapp<appArray.length; iapp++) {
					String appNamez=appArray[iapp];
					log.debug("CAD-054 Adding application "+appNamez);
					applicationNames.add(appNamez.trim());
				}
			}
			else {
				log.debug("CAD-055 did not find a comma, just adding single value");
				applicationNames.add(appName);
			}
			log.debug("CAD-056 Applications:"+applicationNames.toString());
		}
		catch (Exception ex4) {
			log.error("CAD-060 "+ex4.getClass().getName()+":"+ex4.getMessage());
		}
		for (String appNamex:applicationNames) {
			Application app=context.getObjectByName(Application.class,appNamex);
			if(app==null) {
				log.debug("CAD-057 Did not find application "+appNamex);
			}
			else {
				applications.add(app);
				log.debug("CAD-058 Added application: "+app.getName());
			}
		}
		compareMapNameStr=args.getString("compareMap");
		log.debug("CAD-003 Field map custom object:"+compareMapNameStr);
		/*
		 * User can select individual identities or choose the search method.
		 */
		if(args.containsKey("identities")) {
			identities = args.getList("identities");
			log.debug("CAD-005 Identities List:" + identities.toString());
		}
		getAllIdentities=args.getBoolean("selectall");
		/*
		 * onlyActiveIdentities will add a filter on the search to only have inactive=false
		 */
		onlyActiveIdentities=args.getBoolean("onlyActiveIdentities");
		log.debug("CAD-018 onlyActiveIdentities="+onlyActiveIdentities.toString());
		/*
		 * onlyCorrelatedIdentities will add a filter on the search to only have correlated=true
		 */
		onlyCorrelatedIdentities=args.getBoolean("onlyCorrelatedIdentities");
		log.debug("CAD-019 onlyCorrelatedIdentities="+onlyCorrelatedIdentities.toString());
		/*
		 * Determine if this is being executed on a Unix machine
		 */
		String folderSep=File.separator;
		if(folderSep.equals("/")) {
			unixOrigin=true;
		}
		log.debug("CAD-007 Unix Origin="+unixOrigin);
		anyOfTheseFilter=args.getBoolean("anyOfThese");
		if(args.containsKey("folderpath")) {
			outputFolderpath=args.getString("folderpath");
		}
		else {
			if(unixOrigin) {
				outputFolderpath="~"+folderSep+"comparedata";
			}
			else {
				outputFolderpath="C:"+folderSep+"CompareData";
			}
		}
		String dateTag="$date$";
		if(outputFolderpath.contains(dateTag)) {
			DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
			String bpdfStr=bpdf.format(now);
			outputFolderpath=outputFolderpath.replace(dateTag, bpdfStr);
			log.debug("CAD-024 dateTag substitution applied, result:"+outputFolderpath);
		}
		File basePathObj=new File(outputFolderpath);
		if(basePathObj.exists()) {
			log.debug("CAD-024 The folderName "+outputFolderpath+" exists");
		}
		else {
			if(basePathObj.mkdirs()) {
				log.debug("CAD-024 Successfully created "+outputFolderpath);
			}
			else {
				log.error("CAD-024 Count not create folder "+outputFolderpath);
				taskResult.setCompletionStatus(TaskResult.CompletionStatus.Error);
				taskResult.addMessage(new Message(Message.Type.Error,"Could not create output folder"));
				return;
			}
		}
		if(args.containsKey("filename")) {
			outputFilename=args.getString("filename");
		}
		else {
			outputFilename="compare";
		}
		if(outputFilename.contains(dateTag)) {
			DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
			String bpdfStr=bpdf.format(now);
			outputFilename=outputFilename.replace(dateTag, bpdfStr);
			log.debug("CAD-024 dateTag substitution applied, result:"+outputFilename);
		}
		if(outputFilename.endsWith(".xlsx")) {
			log.debug("CAD-024 outputFilenameStr already ends with excel file extension");
		}
		else {
			outputFilename=outputFilename+".xlsx";
			log.debug("CAD-024 outputFilename added excel file extension");
		}
		if(outputFolderpath.endsWith(folderSep)) {
			outputFilenameStr=outputFolderpath+outputFilename;
		}
		else {
			outputFilenameStr=outputFolderpath+folderSep+outputFilename;
		}
		if (!terminate) {
			log.debug("CAD-008 initializing from inputs");
		}
		else {
			log.error("CAD-009 Terminate chosen");
			taskResult.setCompletionStatus(TaskResult.CompletionStatus.Warning);
			taskResult.addMessage(new sailpoint.tools.Message(sailpoint.tools.Message.Type.Info,"Cancelled by user input"));
			return;
		}
		showAllData=args.getBoolean("showAllData");
		/*
		 * Update status
		 */
		sboutput.append("\nInitialized");
		/*
		 * In some cases we might only want to process some identities
		 */
		String identityName="";
		Identity identity=null;
		if(identities!=null && !identities.isEmpty()) {
			for(Object ident: identities) {
				log.debug("CAD-008 Identity class:"+ident.getClass().getName());
				if(ident instanceof String) {
					if ( ((String)ident).contains(",") ) {
						for( String workingIdentity : ((String)ident).split( "," ) ) {
							identityName = ((String)workingIdentity).trim();
							log.debug("CAD-009 Identity selected:"+identityName);
							identity = context.getObjectByName(Identity.class, identityName);
							if (identity != null ) {
								log.debug("CAD-010 identityName="+identity.getName());
							}
							else {
								log.debug("CAD-011 Null Identity Returned for: " + identityName);
							}
							processIdentities.add(identity.getId());
							context.decache(identity);
						}
					}
					else {
						identityName = (String)ident;
						log.debug("CAD-012 Identity selected:"+identityName);
						identity = context.getObjectByName(Identity.class, identityName);
						if (identity != null) {
							log.debug("CAD-013 identityName="+identity.getName());
						}
						else {
							log.debug("CAD-014 Null Identity Returned for: " + identityName);
						}
						processIdentities.add(identity.getId());
						context.decache(identity);
					}
				}
			}
		}
		log.debug("CAD-015 processIdentities after update="+processIdentities.toString());
		if(processIdentities.size()>0) {
			if(processIdentities.size()==1) {
				updateProgress(context, result, "Identity selected: "+identityName);
			}
			else {
				updateProgress(context, result, "Multiple Identities selected");
			}
		}
		log.debug("CAD-016 identities="+processIdentities.toString());
		/*
		 * Read in the Custom data map
		 * Organized by:
		 */
		Custom compareMapCustom=null;
		Attributes compareMapAttrs=null;
		if(Util.isNotNullOrEmpty(compareMapNameStr)) {
			compareMapCustom = context.getObjectByName(Custom.class,compareMapNameStr);
			if(compareMapCustom==null) {
				log.error("CAD-009 Field Map custom object is missing");
				taskResult.setCompletionStatus(TaskResult.CompletionStatus.Error);
				taskResult.addMessage(new sailpoint.tools.Message(sailpoint.tools.Message.Type.Error,"Field Map custom object is missing"));
				return;
			}
			else {
				compareMapAttrs=compareMapCustom.getAttributes();
				compareMapOrig=compareMapAttrs.getMap();
				for(String compareMapKey: compareMapOrig.keySet()) {
					Object compareMapValue=compareMapOrig.get(compareMapKey);
					compareMapObj.put(compareMapKey,compareMapValue);
				}
			}
		}
		/*
		log.debug("CAD-030 Creating histogramMap");
		Map<String,Integer> histogramMap=new HashMap<String,Integer>();
		 */
		if(getAllIdentities) {
			log.debug("CAD-031 Creating QueryOptions and Filter");
			QueryOptions qo=new QueryOptions();
			/*
			* First, need to establish a filter on identities that have
			* all of the specified applications.	Most of the time there
			* will be two but sometimes more.
			*/
			List<Filter> filterList=new ArrayList<Filter>();
			Filter appFilter=null;
			int appCount=0;
			for (String appNamey: applicationNames) {
				appCount++;
				if(appCount==1) {
					appFilter=Filter.eq("links.application.name",appNamey);
					filterList.add(appFilter);
					log.debug("CAD-041 Adding application filter for "+appNamey);
				}
			}
			/*
			* If onlyActiveIdentities, add that filter
			*/
			if(onlyActiveIdentities.booleanValue()) {
				Filter activeFilter=Filter.eq("inactive", false);
				filterList.add(activeFilter);
				log.debug("CAD-046 added filter for inactive=false");
			}
			/*
			* If onlyCorrelatedIdentities, add that filter
			*/
			if(onlyCorrelatedIdentities.booleanValue()) {
				Filter correlatedFilter=Filter.eq("correlated", true);
				filterList.add(correlatedFilter);
				log.debug("CAD-047 added filter for correlated=true");
			}
			if(args.containsKey("filterString")) {
				filterString = args.getString("filterString");
				log.debug("CAD-030 filterString="+filterString);
			}
			else {
				log.debug("CAD-031 did not find filterString in args");
			}
			if(Util.isNotNullOrEmpty(filterString)) {
				log.debug("CAD-045 filterString specified, using it:"+filterString);
				Filter compiledFilter=Filter.compile(filterString);
				filterList.add(compiledFilter);
			}
			if(filterList.size()==1) {
				qo.addFilter(appFilter);
				log.debug("CAD-044 Adding appFilter");
			}
			else {
				qo.addFilter(Filter.and(filterList));
				log.debug("CAD-048 Adding and filter on the filter list");
			}
			if(args.containsKey("orderby")) {
				orderbyString = args.getString("orderby");
				log.debug("EID-032 orderby="+orderbyString);
			}
			else {
				log.debug("EID-033 did not find orderbyString in args");
			}
			if(Util.isNotNullOrEmpty(orderbyString)) {
				log.debug("EID-042 Found an orderbyString: "+orderbyString);
				if(orderbyString.contains(":")) {
					String[] orderByArray=orderbyString.split(":");
					qo.setOrderBy(orderByArray[0]);
					if("desc".equals(orderByArray[1])) {
						qo.setOrderAscending(false);
					}
				}
				else {
					qo.setOrderBy(orderbyString);
				}
			}
			else {
				log.debug("EID-043 Did not find an orderbyString, using name");
				qo.setOrderBy("name");
			}
			log.debug("CAD-032 Creating IncrementalObjectIterator using filter:"+qo.toString());
			IncrementalObjectIterator iter=new IncrementalObjectIterator(context,Identity.class,qo);
			if(iter!=null) {
				while(iter.hasNext()) {
					Identity identObj = (Identity)iter.next();
					log.debug("CAD-035 Scanning: "+identObj.getName()+" : "+identObj.getDisplayName());
					sboutput.append("\n\nScanning: "+identObj.getName()+" : "+identObj.getDisplayName());
					boolean includeIdentity=true;
					if(!(anyOfTheseFilter)) {
						for(Application appObj: applications) {
							int acctCount=idService.countLinks(identObj,appObj);
							if (acctCount != 1) {
								includeIdentity=false;
							}
						}
					}
					if(includeIdentity) {
						processIdentities.add(identObj.getId());
					}
					context.decache(identObj);
				}
			}
		}
		int count=0;
		int itercount=0;
		String fieldValue="";
		/*
		Map<String,Integer> typeMap=new HashMap<String,Integer>();
		log.debug("CAD-033 Have iterator, creating maps");
		 */
		updateProgress(context, result, "Creating analysis maps");
		
		log.debug("CAD-034 initializing identityList");
		List<String> identityList=new ArrayList<String>();
		
		/*
		 * Figure out where to send the results
		 */
		if(args.containsKey("resultsOutput")) {
			resultsOutputStr=args.getString("resultsOutput");
			if(resultsOutputStr.contains("@")) {
				// this is an email address !!
				sendToEmail=true;
			}
		}
		if(!processIdentities.isEmpty()) {
			/*
			 * Open the workbook and sheet
			 */
			Workbook wb = null;
			OutputStream fileOut = null;
			Map<Integer,Row> rowMap=new HashMap<Integer,Row>();
			Integer rowOrdinal=null;
			int rowNumber=0;
			int columnNumber=0;
			int maxColumnNumber=0;
			boolean advanceRow=true;
			try {
				log.debug("CAD-070 creating a new XSSFWorkbook");
				wb = new XSSFWorkbook();
				fileOut = new FileOutputStream(outputFilenameStr);
				CreationHelper createHelper = wb.getCreationHelper();
				log.debug("CAD-071 Creating a new sheet with the name "+nowTimeLongStr);
				CellStyle baseCellStyle=wb.createCellStyle();
				CellStyle highlightStyle=wb.createCellStyle();
				highlightStyle.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
				highlightStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
				
				Sheet sheet = wb.createSheet(nowTimeLongStr);
				
				log.debug("CAD-072 Creating the first row");
				Row row = sheet.createRow(rowNumber);
				rowOrdinal=Integer.valueOf(rowNumber);
				rowMap.put(rowOrdinal,row);
				columnNumber=0;
				Cell cell = row.createCell(columnNumber);
				cell.setCellValue("CompareAccountData");
				cell.setCellStyle(baseCellStyle);
				log.debug("CAD-073 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				cell = row.createCell(columnNumber+1);
				cell.setCellValue("task output");
				cell.setCellStyle(baseCellStyle);
				log.debug("CAD-073 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				
				rowNumber++;
				columnNumber=0;
				row = getOrCreateRow(sheet, rowNumber, rowMap);
				cell = row.createCell(columnNumber);
				cell.setCellValue("Run time:");
				cell.setCellStyle(baseCellStyle);
				log.debug("CAD-074 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				cell = row.createCell(columnNumber+1);
				cell.setCellValue(sdfout.format(now));
				cell.setCellStyle(baseCellStyle);
				log.debug("CAD-074 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				
				rowNumber++;
				columnNumber=0;
				row = getOrCreateRow(sheet, rowNumber, rowMap);
				cell = row.createCell(columnNumber);
				cell.setCellValue("Applications:");
				cell.setCellStyle(baseCellStyle);
				log.debug("CAD-075 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				for (String appNameValue: applicationNames) {
					columnNumber++;
					cell = row.createCell(columnNumber);
					cell.setCellValue(appNameValue);
					cell.setCellStyle(baseCellStyle);
					log.debug("CAD-075 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				}
				
				rowNumber++;
				columnNumber=0;
				row = getOrCreateRow(sheet, rowNumber, rowMap);
				cell = row.createCell(columnNumber);
				cell.setCellValue("Compare Map:");
				cell.setCellStyle(baseCellStyle);
				log.debug("CAD-076 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				cell = row.createCell(columnNumber+1);
				cell.setCellValue(compareMapNameStr);
				cell.setCellStyle(baseCellStyle);
				log.debug("CAD-076 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				
				rowNumber=2+rowNumber;
				columnNumber=0;
				row = getOrCreateRow(sheet, rowNumber, rowMap);
				columnNumber=2;
				maxColumnNumber=2;
				for (String searchKey: compareMapObj.keySet()) {
					cell=row.createCell(columnNumber);
					cell.setCellValue(searchKey);
					cell.setCellStyle(baseCellStyle);
					log.debug("CAD-077 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
					sheet.addMergedRegion(new CellRangeAddress(rowNumber,rowNumber,columnNumber,columnNumber+1));
					CellUtil.setAlignment(cell, HorizontalAlignment.CENTER);
					columnNumber=2+columnNumber;
					if(columnNumber>maxColumnNumber)maxColumnNumber=columnNumber;
				}
				
				rowNumber++;
				columnNumber=0;
				row = getOrCreateRow(sheet, rowNumber, rowMap);
				cell=row.createCell(columnNumber);
				cell.setCellValue("Identities with differences");
				cell.setCellStyle(baseCellStyle);
				log.debug("CAD-078 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
				row = getOrCreateRow(sheet, rowNumber+1, rowMap);
				cell=row.createCell(columnNumber);
				cell.setCellValue("Identity name");
				cell.setCellStyle(baseCellStyle);
				cell=row.createCell(columnNumber+1);
				cell.setCellValue("Display name");
				cell.setCellStyle(baseCellStyle);
				row = getOrCreateRow(sheet, rowNumber, rowMap);
				
				columnNumber=2;
				/*
				 * Look at the map.  It might look like this:
				 * <entry key="HJK-FIRSTNAME">
				 *  <value>
				 *    <Map>
				 *      <entry key="BASE_APP" value="HR Employees"/>
				 *      <entry key="HR Employees" value="firstName"/>
				 *      <entry key="HR Employees2" value="firstName"/>
				 *    </Map>
				 *  </value>
				 * </entry>
				 * or it might be same without the BASE_APP.  BASE_APP specifies that
				 * this application is always the left side and the reference.
				 * otherwise it's just random order since it's a map.
				 */
				for (String searchKey: compareMapObj.keySet()) {
					// For each value of searchKey there can be a different set of applications
					// and a different baseApp
					SortedSet<String> sortedAppNames=new TreeSet<String>();
					String baseApp=null;
					String appNameValue=null;
					// For the example searchKey is HJK-FIRSTNAME and is at the top of the 6 cell block.
					Object compareMapValueObj = compareMapObj.get(searchKey);
					// bare maps always come back <String,Object>
					Map compareMapValue = new HashMap();
					if(compareMapValueObj instanceof Map) {
						compareMapValue = (Map)compareMapValueObj;
					}
					// Build the baseApp and sortedAppNames objects
					if(compareMapValue.containsKey("BASE_APP")) {
						// Get the name of the base app, in the example it is HR Employees
						baseApp=CommonMethods.normalizeToString(compareMapValue.get("BASE_APP"));
						log.debug("CAD-079 In the map found BASE_APP = "+baseApp);
					}
					// Build the sorted app names set
					for(Object cmKeyObj: compareMapValue.keySet()) {
						String cmKey=CommonMethods.normalizeToString(cmKeyObj);
						if(cmKey.equals("BASE_APP")) {
							continue;
						}
						else {
							if(baseApp==null) {
								sortedAppNames.add(cmKey);
							}
							else {
								if(cmKey.equals(baseApp)) {
									continue;
								}
								else {
									sortedAppNames.add(cmKey);
								}
							}
						}
					}
					log.debug("CAD-078 sortedAppNames set = "+sortedAppNames.toString());
					// Now process
					if(baseApp!=null) {
						// Write this to the first column in the second (current) row
						cell=row.createCell(columnNumber);
						cell.setCellValue(baseApp);
						cell.setCellStyle(baseCellStyle);
						log.debug("CAD-080 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
						// Now write the field name to the next row
						row=getOrCreateRow(sheet, rowNumber+1, rowMap);
						cell=row.createCell(columnNumber);
						cell.setCellValue(CommonMethods.normalizeToString(compareMapValue.get(baseApp)));
						cell.setCellStyle(baseCellStyle);
						log.debug("CAD-081 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
						// Reset the row value to the previous row
						row=getOrCreateRow(sheet, rowNumber, rowMap);
						// increment the column number
						columnNumber++;
						if(columnNumber>maxColumnNumber)maxColumnNumber=columnNumber;
					}
					//for(String cmKey: compareMapValue.keySet()) {
					for(String cmKey: sortedAppNames) {
						cell=row.createCell(columnNumber);
						cell.setCellValue(cmKey);
						cell.setCellStyle(baseCellStyle);
						log.debug("CAD-082 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
						row=getOrCreateRow(sheet, rowNumber+1, rowMap);
						cell=row.createCell(columnNumber);
						cell.setCellValue(CommonMethods.normalizeToString(compareMapValue.get(cmKey)));
						cell.setCellStyle(baseCellStyle);
						log.debug("CAD-083 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
						row=getOrCreateRow(sheet, rowNumber, rowMap);
						columnNumber++;
						if(columnNumber>maxColumnNumber)maxColumnNumber=columnNumber;
					}
				}
				rowNumber++;
				for(String identId: processIdentities) {
					advanceRow=true;
					Identity identObj = context.getObjectById(Identity.class, identId);
					log.debug("CAD-035 Scanning: "+identObj.getName()+" : "+identObj.getDisplayName());
					sboutput.append("\n\nScanning: "+identObj.getName()+" : "+identObj.getDisplayName());
					Link link=null;
					String nativeIdentity=null;
					List<Link> links = idService.getLinks(identObj,0,0);
					Map<String,Link> linkMap = new HashMap<String,Link>();
					Map<String,String> nativeIdMap = new HashMap<String,String>();
					for(Link alink: links) {
						String linkAppName=alink.getApplicationName();
						if (linkMap.containsKey(linkAppName)) {
							log.debug("CAD-036 Found an additional account link for "+linkAppName+" : "+alink.getNativeIdentity());
						}
						else {
							linkMap.put(linkAppName,alink);
							nativeIdentity=alink.getNativeIdentity();
							log.debug("CAD-037 Found account "+nativeIdentity);
							nativeIdMap.put(linkAppName,nativeIdentity);
						}
					}
					itercount++;
					/*
					* Now analyze according to the custom object
					* Each comparison will be prefaced with a tag
					* and include a map stating which application field to compare
					*
					* for instance:
					* <entry name="IDA-FirstName">
					*	 <value>
					*		 <Map>
					*			 <entry name="Workday Direct" value="FIRST_NAME"/>
					*			 <entry name="Workday File" value="FIRSTNAME"/>
					*			 <entry name="IdentityIQ" value="firstname"/>	to include an identity field in the search
					*						You can search value of a connector versus an identity field this way too
					*/
					columnNumber=2;
					for (String searchKey: compareMapObj.keySet()) {
						log.debug("CAD-051 checking searchKey = "+searchKey);
						Object searchMapObj = compareMapObj.get(searchKey);
						Map<String,String> searchMap = null;
						if(searchMapObj instanceof Map) {
							searchMap=(Map<String,String>)searchMapObj;
							log.debug("CAD-052 search map = "+searchMap.toString());
						}
						else {
							log.error("CAD-052 for "+searchKey+", searchMapObj is not a map, please fix");
							break;
						}
						SortedSet<String> sortedAppNames=new TreeSet<String>();
						String baseApp=null;
						String appNameValue=null;
						// For the example searchKey is HJK-FIRSTNAME and is at the top of the 6 cell block.
						Object compareMapValueObj = compareMapObj.get(searchKey);
						// bare maps always come back <String,Object>
						Map compareMapValue = new HashMap();
						if(compareMapValueObj instanceof Map) {
							compareMapValue = (Map)compareMapValueObj;
						}
						// Build the baseApp and sortedAppNames objects
						if(compareMapValue.containsKey("BASE_APP")) {
							// Get the name of the base app, in the example it is HR Employees
							baseApp=CommonMethods.normalizeToString(compareMapValue.get("BASE_APP"));
							log.debug("CAD-085 In the map found BASE_APP = "+baseApp);
						}
						// Build the sorted app names set
						for(Object cmKeyObj: compareMapValue.keySet()) {
							String cmKey=CommonMethods.normalizeToString(cmKeyObj);
							if(cmKey.equals("BASE_APP")) {
								continue;
							}
							else {
								if(baseApp==null) {
									sortedAppNames.add(cmKey);
								}
								else {
									if(cmKey.equals(baseApp)) {
										continue;
									}
									else {
										sortedAppNames.add(cmKey);
									}
								}
							}
						}
						log.debug("CAD-086 sortedAppNames set = "+sortedAppNames.toString());
						int ordinal=0;
						// compareList is a list of applications
						// compareMap is 
						List<String> compareList=new ArrayList<String>();
						if(baseApp!=null) {
							compareList.add(baseApp);
						}
						for(String cmKey: sortedAppNames) {
							compareList.add(cmKey);
						}
						log.debug("CAD-086 loaded compareList and creating compareMap: "+compareList.toString());
						Map<String,String> compareMap=new HashMap<String,String>();
						for (String appKey: compareList) {
							/*
							 * The first application in the list is considered the reference
							 * unless BASE_APP is set
							 */
							String attrName = searchMap.get(appKey);
							Object fieldValueObj=null;
							String fieldValueStr="";
							String firstStr="";
							if("IdentityIQ".equals(appKey)) {
								fieldValueObj = CommonMethods.getIdentityAttribute(identObj,attrName);
							}
							else {
								if(linkMap.containsKey(appKey)) {
									link = linkMap.get(appKey);
									fieldValueObj = link.getAttribute(attrName);
								}
								else {
									fieldValueObj = "";
								}
							}
							fieldValueStr=CommonMethods.normalizeToString(fieldValueObj);
							ordinal++;
							compareMap.put(appKey,fieldValueStr);
							log.debug("CAD-053 Added ["+appKey+","+fieldValueStr+"] to compareMap");
						}
						boolean differencesFound=false;
						String initialValue=null;
						for (String appKey : compareList) {
							String appValue=compareMap.get(appKey);
							if(initialValue==null) {
								initialValue = appValue;
							}
							else {
								if(!(appValue.equals(initialValue))) {
									differencesFound=true;
								}
							}
						}
						if(differencesFound || showAllData) {
							if(advanceRow) {
								if(differencesFound) {
									sboutput.append("\nDifferences found on identity: "+identObj.getName()+" : "+identObj.getDisplayName());
								}
								rowNumber++;
								row=getOrCreateRow(sheet, rowNumber, rowMap);
								cell=row.createCell(0);
								cell.setCellValue(identObj.getName());
								cell.setCellStyle(baseCellStyle);
								log.debug("CAD-087 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
								cell=row.createCell(1);
								cell.setCellValue(identObj.getDisplayName());
								cell.setCellStyle(baseCellStyle);
								log.debug("CAD-088 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
								advanceRow=false;
							}
							int appcol=0;
							for (String appKey : compareList) {
								String appValue=compareMap.get(appKey);
								// sboutput.append("\n"+String.format("%40s",appKey)+" : "+appValue);
								cell=row.createCell(columnNumber+appcol);
								cell.setCellValue(appValue);
								if(showAllData && differencesFound) {
									cell.setCellStyle(highlightStyle);
								}
								else {
									cell.setCellStyle(baseCellStyle);
								}
								log.debug("CAD-089 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
								appcol++;
							}
						}
						columnNumber=2+columnNumber;
						if(columnNumber>maxColumnNumber)maxColumnNumber=columnNumber;
					}
					if((itercount%100)==100) {
						updateProgress(context, result, String.format("%6d",itercount)+" "+nativeIdentity);
					}
					context.decache(identObj);
				}
				for (int columnIndex=0; columnIndex < maxColumnNumber; ++columnIndex) {
					sheet.autoSizeColumn(columnIndex);
					log.debug("CAD-084 Column number: "+columnIndex+" has width: "+sheet.getColumnWidth(columnIndex));
				}
			}
			catch (Exception fex) {
				log.error("CAD-198 "+fex.getClass().getName()+":"+fex.getMessage());
				fex.printStackTrace();
			}
			finally {
				if(fileOut!=null) {
					try {
						wb.write(fileOut);
						fileOut.flush();
						fileOut.close();
						successfulFileWrite=true;
					}
					catch (Exception gex) {
						log.error("CAD-199 "+gex.getClass().getName()+":"+gex.getMessage());
					}
				}
			}
		}
		else {
			log.debug("CAD-038 iter is null");
			sboutput.append("\niter is null");
		}
		if(successfulFileWrite && sendToEmail) {
			log.debug("CAD-090 going to send email to:"+resultsOutputStr);
			Properties props=new Properties();
			props.put("mail.smtp.auth", false);
			props.put("mail.smtp.starttls.enable", "false");
			Configuration sysConfig=context.getObjectByName(Configuration.class,"SystemConfiguration");
			String smtpServer=sysConfig.DEFAULT_EMAIL_HOST;
			log.debug("CAD-091 smtpServer="+smtpServer);
			props.put("mail.smtp.host", smtpServer);
			String smtpPort=sysConfig.DEFAULT_EMAIL_PORT;
			log.debug("CAD-092 smtpPort="+smtpPort); 
			props.put("mail.smtp.port", smtpPort);
			Session session = Session.getInstance(props);
			javax.mail.Message msg=new MimeMessage(session);
			String fromAddress=sysConfig.DEFAULT_EMAIL_FROM;
			log.debug("CAD-093 fromAddress="+fromAddress);
			msg.setFrom(InternetAddress.parse(fromAddress)[0]);
			msg.setRecipients(
				javax.mail.Message.RecipientType.TO, InternetAddress.parse(resultsOutputStr));
			DataSource fds=new FileDataSource(outputFilenameStr);
			MimeBodyPart mbp1 = new MimeBodyPart();
			mbp1.setText("Output file");
			MimeBodyPart mbp2 = new MimeBodyPart();
			mbp2.setDataHandler(new DataHandler(fds));
			Multipart mp = new MimeMultipart();
			mp.addBodyPart(mbp1);
			mp.addBodyPart(mbp2);
			msg.setContent(mp);
			msg.saveChanges();
			// Set the Date: header
			msg.setSentDate(new java.util.Date());
			Transport.send(msg);
		}
		sboutput.append("\n\nprocessed");
		/*
		 * Display the results
		 */
		result.setAttribute("totalCount", totalCount);
		result.setAttribute("resultString", sboutput.toString());
		taskResult.setCompletionStatus(TaskResult.CompletionStatus.Success);
		taskResult.addMessage(new sailpoint.tools.Message(sailpoint.tools.Message.Type.Info,"Processed"));
	}
	private Row getOrCreateRow(Sheet s, int rowNum, Map<Integer,Row> rm) {
		Integer rowOrdinal=Integer.valueOf(rowNum);
		if(rm.containsKey(rowOrdinal)) {
			return rm.get(rowOrdinal);
		}
		Row row=s.createRow(rowNum);
		rm.put(rowOrdinal,row);
		return row;
	}
	private void changeCellBackgroundColor(Cell cell) {
		CellStyle cellStyle = cell.getCellStyle();
		if(cellStyle == null) {
			cellStyle = cell.getSheet().getWorkbook().createCellStyle();
		}
		cellStyle.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
		cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		cell.setCellStyle(cellStyle);
	}
	
	public boolean terminate() {
		terminate=true;
		taskResult.setTerminated(true);
		if (log.isDebugEnabled())
			log.debug("Task was terminated."); 
		return true;
	}
	
	public String getPluginName() {
		return PLUGIN_NAME;
	}
}
