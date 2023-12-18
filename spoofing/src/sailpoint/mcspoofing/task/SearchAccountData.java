package sailpoint.mcspoofing.task;

import sailpoint.mcspoofing.common.CommonMethods;
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Stack;
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
import sailpoint.api.ObjectUtil;
import sailpoint.api.IdentityService;
import sailpoint.Version;
import sailpoint.object.Application;
import sailpoint.object.Attributes;
import sailpoint.object.AuditConfig;
import sailpoint.object.AuditConfig.AuditAction;
import sailpoint.object.AuditConfig.AuditAttribute;
import sailpoint.object.AuditConfig.AuditClass;
import sailpoint.object.Bundle;
import sailpoint.object.ClassLists;
import sailpoint.object.Configuration;
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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
/**
 * Process inactive users from applications
 *
 * @author Keith Smith
 */
public class SearchAccountData extends BasePluginTaskExecutor {
  private static final String PLUGIN_NAME = "MCSpoofingPlugin";
  
  private static Log log = LogFactory.getLog(SearchAccountData.class);
  private boolean terminate=false;
  private boolean unixOrigin=false;
  private int totalCount=0;
  private long oldestDate=0L;
  private StringBuffer sboutput=new StringBuffer();
  private String appName="";
  private String fieldName="";
  private boolean fieldIsBoolean=false;
  private String searchValue="";
  private String compareField=null;
  private boolean compareIsBoolean=false;
  /*
   * newAttribute, newValue, setNewValue, setAll, del
   * all are to support the modify values functionality
   */
  private String newAttribute=null;
  private boolean newAttributeIsBoolean=false;
  private String newValue=null;
  private boolean setNewValue=false;
  private boolean setAllNewValues=false;
  private boolean deleteNewValue=false;
  private boolean ignoreCase=true;
  private boolean ignoreNullsOnCompare=false;
  private boolean genHistogram=false;
  private boolean checkCorr=true;
  private boolean genIdentityList=false;
  private boolean genIdentityFilter=false;
  private String multiSearch="";
  private boolean multiSearchIsFilter=false;

  private List<Application> applications=new ArrayList<Application>();
  private Boolean onlyActiveIdentities=true;
  private Boolean onlyCorrelatedIdentities=true;
  /*
   * useSQL, version, database are all for database support
   */
  private Boolean useSQL=false;
  private String sailpointVersion="";
  private String sailpointPatch="";
  private String databaseType="";
  private String stringLengthFunction="";
  /*
   * histosort and listModulo are new functionality
   */
  private String histoSort="desc";
  private String listModuloStr="1";
  private int listModulo=1;
  /*
   * To support changing values
   */
  private boolean matchedValue=false;
  private boolean addToList=false;
  private Object newAttributeValueObj=null;
  private String newAttributeValueStr=null;
  
  private TaskResult taskResult;
  @SuppressWarnings({"rawtypes","unchecked"})
  @Override
  public void execute(SailPointContext context, TaskSchedule schedule,
    TaskResult result, Attributes args) throws Exception {
    DateFormat sdfout=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    DateFormat sdfnotime=new SimpleDateFormat("MM/dd/yyyy");
    Date now=new Date();
    Date nowNoTime=now;
  /*
   * Some fields in the database are version specific
   * So we need to know the version we are on
   */
    sailpointVersion=sailpoint.Version.getVersion();
    sailpointPatch=sailpoint.Version.getPatchLevel();
    log.debug("SAD-000 Version "+sailpointVersion+" patch level "+sailpointPatch);
    String nowDateNoTimeStr=sdfnotime.format(now);
    try {
      nowNoTime=sdfnotime.parse(nowDateNoTimeStr);
    }
    catch (Exception et) {}
    oldestDate=nowNoTime.getTime();
    sboutput.append("SearchAccountData started at "+sdfout.format(now));
    log.debug("SAD-001 Starting Search Account Data");
    taskResult=result;
    /*
     * Gather inputs
     * appName         is the application to be searched
     * fieldName       is the single field you want to look at
     *                 normally you are wanting to search for a certain value, wanting a histogram
     *                 of that field, or both.  That is the first use case.
     * searchValue     is set to the value to be searched in that case or left
     *                 blank in the case of generating a histogram.
     *                 If leaving this blank you will at least count the number of
     *                 non null values
     * ignoreCase      means to ignore case in the compare
     * genHistogram    means to generate a histogram of the values found
     * multiSearch     this is something I tried but needs more definition
     * genIdentityList means to print out the identity name and display name of matches
     * compareField    is another field in this same application to compare to the fieldName values
     *                 and instead of finding matches, finds differencesl.
     * ignoreNullsOnCompare means to ignore nulls ?? not sure what that was supposed to mean.
     */

    boolean terminateMissingInput=false;
    if(args.containsKey("appName")) {
      appName=args.getString("appName");
      log.debug("SAD-002 Application name:"+appName);
    }
    else {
      terminateMissingInput=true;
      log.error("SAD-002 Application name is missing");
    }
    
    if(args.containsKey("fieldName")) {
      fieldName=args.getString("fieldName");
      if(fieldName.toLowerCase().startsWith("boolean:")) {
        fieldIsBoolean=true;
        fieldName=fieldName.substring(8);
      }
      log.debug("SAD-003 Field name:"+fieldName);
    }
    else {
      terminateMissingInput=true;
      log.debug("SAD-003 Field name is missing");
    }
    
    if(args.containsKey("searchValue")) {
      searchValue=args.getString("searchValue");
      log.debug("SAD-004 Search value:"+searchValue);
    }
    
    ignoreCase=args.getBoolean("ignoreCase");
    log.debug("SAD-005 Ignore Case:"+ignoreCase);
    
    genHistogram=args.getBoolean("genHistogram");
    log.debug("SAD-006 Generate Histotram:"+genHistogram);
    
    if(args.containsKey("multiSearch")) {
      multiSearch=args.getString("multiSearch");
      log.debug("SAD-007 multiSearch:"+multiSearch);
    }
    
    genIdentityList=args.getBoolean("genIdentityList");
    log.debug("SAD-008 Generate Identity List:"+genIdentityList);
  
  /*
   * List modulo is new to only print every N'th identity
   */
    if(args.containsKey("listModulo")) {
      listModuloStr=args.getString("listModulo");
      log.debug("SAD-009 List Modulo specified as "+listModuloStr);
    }
    
    genIdentityFilter=args.getBoolean("genIdentityFilter");
    log.debug("SAD-008 Generate Identity Filter:"+genIdentityFilter);
    
    if(args.containsKey("compareField")) {
      compareField=args.getString("compareField");
      if(compareField.toLowerCase().startsWith("boolean:")) {
        compareIsBoolean=true;
        compareField=compareField.substring(8);
      }
      log.debug("SAD-027 Compare Field="+compareField);
      if(genHistogram) {
        sboutput.append("\nCompare field specified: "+compareField+", histogram is not allowed");
        genHistogram=Boolean.valueOf(false);
      }
    }
  /*
   * histoSort is the histogram sort algorithm
   * value means sort alphabetically by value
   * desc means sort by count, descending, then alpha by value
   * asc means sort by count, ascending, then alpha by value
   */
    if(args.containsKey("histoSort")) {
      histoSort=args.getString("histoSort");
      log.debug("SAD-161 Histogram Sort="+histoSort);
    }
    
    ignoreNullsOnCompare=args.getBoolean("ignoreNulls");
    log.debug("SAD-059 Ignore Nulls on Compare:"+ignoreNullsOnCompare);

    /*
   * newAttribute means to set the value for an attribute
   */
    if(args.containsKey("newAttribute")) {
      newAttribute=args.getString("newAttribute");
      if(newAttribute.toLowerCase().startsWith("boolean:")) {
        newAttributeIsBoolean=true;
        newAttribute=newAttribute.substring(8);
      }
      log.debug("SAD-064 Set values on this attribute:"+newAttribute);
      if(args.containsKey("newValue")) {
        newValue=args.getString("newValue");
        log.debug("SAD-060 Set values to this value:"+newValue);
      }
    }
    if(newValue!=null && newAttribute!=null) {
      setNewValue=true;
      if(newValue.startsWith("all:")) {
        setAllNewValues=true;
        newValue=newValue.substring(4);
        log.debug("SAD-066 Setting all values of "+newAttribute+" to "+newValue);
      }
      if(newValue.equals("NULL")) {
        deleteNewValue=true;
      }
    }
    /*
     * onlyActiveIdentities will add a filter on the search to only have inactive=false
     */
    onlyActiveIdentities=args.getBoolean("onlyActiveIdentities");
    log.debug("EID-018 onlyActiveIdentities="+onlyActiveIdentities.toString());
    /*
     * onlyCorrelatedIdentities will add a filter on the search to only have correlated=true
     */
    onlyCorrelatedIdentities=args.getBoolean("onlyCorrelatedIdentities");
    log.debug("EID-019 onlyCorrelatedIdentities="+onlyCorrelatedIdentities.toString());
    /*
     * Determine if this is being executed on a Unix machine
     */
    String folderSep=File.separator;
    if(folderSep.equals("/")) {
      unixOrigin=true;
    }
    log.debug("SAD-007 Unix Origin="+unixOrigin);
    if ((!terminate) && (!terminateMissingInput)) {
      log.debug("SAD-008 initializing from inputs");
    }
    else {
      log.error("SAD-009 Terminate chosen or missing inputs");
      taskResult.setCompletionStatus(TaskResult.CompletionStatus.Warning);
      taskResult.addMessage(new Message(Message.Type.Info,"Cancelled or missing inputs"));
      return;
    }
    /*
     * Determine if we should use SQL for this or the API.  This is a plugin setting.
     */
    useSQL=getSettingBool("useSQL");
    /*
     * Update status
     */
    sboutput.append("\nInitialized");
    /*
     * Process the data
     */
    if(Util.isNotNullOrEmpty(fieldName)) {
      if(Util.isNullOrEmpty(compareField)) {
        sboutput.append("\nSearching "+appName+" for links with "+fieldName+" = "+searchValue);
        log.debug("SAD-010 Searching "+appName+" for links with "+fieldName+" = "+searchValue);
        updateProgress(context, result, "Searching "+appName+" for links with "+fieldName+" = "+searchValue);
      }
      else {
        sboutput.append("\nSearching "+appName+" for links with "+fieldName+" != "+compareField);
        log.debug("SAD-029 Searching "+appName+" for links with "+fieldName+" != "+compareField);
        updateProgress(context, result, "Searching "+appName+" for links with "+fieldName+" != "+compareField);
      }
      if(ignoreCase) {
        sboutput.append("\n ignoring case");
      }
    }
    log.debug("SAD-030 Creating histogramMap");
    Map<String,Integer> histogramMap=new HashMap<String,Integer>();
    log.debug("SAD-031 Creating QueryOptions and Filter");
    QueryOptions qo=new QueryOptions();
    List<Filter> filterList=new ArrayList<Filter>();
    int itercount=0;
    int nncount=0;
    int corrcount=0;
    int noncorrcount=0;
    int count=0;
    int rawcount=0;
    log.debug("SAD-034 initializing identityList");
    /*
     * identityList is a list of identity name and display name for matches
     * identityListRaw is a list of identity IDs for all accounts on an application
     * this is why the SQL is better as it limits the number of records pulled.
     */
    List<String> identityList=new ArrayList<String>();
    List<String> identityNameList=new ArrayList<String>();
    List<String> identityListRaw=new ArrayList<String>();
    IdentityService idService=new IdentityService(context);
    Application appObj=context.getObjectByName(Application.class,appName);
    java.sql.Connection sqlConnection=null;
    PreparedStatement pstmt=null;
    String innerQuery=null;
    String outerQuery=null;
    ResultSet queryResult=null;
    boolean goodResults=false;
    if(useSQL) {
      sqlConnection=context.getJdbcConnection();
      DatabaseMetaData dbmd=sqlConnection.getMetaData();
      databaseType=dbmd.getDatabaseProductName();
      log.debug("SAD-160 Database product name="+databaseType);
      if("MySQL".equals(databaseType)) {
        stringLengthFunction="LENGTH";
      }
      else if("Microsoft SQL Server".equals(databaseType)) {
        stringLengthFunction="LEN";
      }
      else if("Oracle".equals(databaseType)) {
        stringLengthFunction="LENGTH";
      }
      /*
       * The inner query generates the value to be used for the outer query
       * ( Select y.native_identity as native_identity
       * , y.iiq_disabled as iiq_disabled
       * , y.identity_id as identity_id 
       * , EXTRACTVALUE(y.attributes, '/Attributes/Map/entry[@key="<fieldName>"]/@value') as <fieldName>
       * from spt_application x, spt_link y
       * where x.name=<application name>
       * and y.application=x.id )
       *
       * The outer query is:
       * select a.id, a.name, a.displayName, a.type, a.inactive, a.correlated
       * , b.native_identity, b.<fieldName>, b.iiq_disabled
       * from spt_identity a, ( ) b
       * where a.id = b.identity_id
       * (optional)
       * and a.type = 
       * and a.inactive = 
       * and a.correlated = 
       */
      try {
        innerQuery = "select y.native_identity as native_identity";
        // Requires 8.2
        if(CommonMethods.isVersionGE(sailpointVersion,sailpointPatch,"8.2","")) {
          innerQuery+= ", y.iiq_disabled as iiq_disabled";
        }
        innerQuery+= ", y.identity_id as identity_id";
        if("MySQL".equals(databaseType)) {
          if(fieldIsBoolean) {
            log.debug("SAD-174 field "+fieldName+" was identified as a boolean");
            innerQuery+= ", EXTRACTVALUE(y.attributes, '/Attributes/Map/entry[@key=\""+fieldName+"\"]/value/Boolean/text()') as "+fieldName.toLowerCase();
          }
          else {
            log.debug("SAD-175 field "+fieldName+" NOT identified as a boolean");
            innerQuery+= ", EXTRACTVALUE(y.attributes, '/Attributes/Map/entry[@key=\""+fieldName+"\"]/@value') as "+fieldName.toLowerCase();
          }
        }
        else if("Microsoft SQL Server".equals(databaseType)) {
          if(fieldIsBoolean) {
            log.debug("SAD-176 field "+fieldName+" was identified as a boolean");
            innerQuery+= ", cast(y.attributes as xml).value('(/Attributes/Map/entry[@key=\""+fieldName+"\"]/value/Boolean/text())[1]','nvarchar(max)') as "+fieldName.toLowerCase();
          } 
          else {
            log.debug("SAD-177 field "+fieldName+" NOT identified as a boolean");
            innerQuery+= ", cast(y.attributes as xml).value('(/Attributes/Map/entry[@key=\""+fieldName+"\"]/@value)[1]','nvarchar(max)') as "+fieldName.toLowerCase();
          }
        }
        else if("Oracle".equals(databaseType)) {
          if(fieldIsBoolean) {
            log.debug("SAD-174A field "+fieldName+" was identified as a boolean");
            innerQuery+= ", EXTRACTVALUE(XMLTYPE(y.attributes), '/Attributes/Map/entry[@key=\""+fieldName+"\"]/value/Boolean/text()') as "+fieldName.toLowerCase();
          }
          else {
            log.debug("SAD-175A field "+fieldName+" NOT identified as a boolean");
            innerQuery+= ", EXTRACTVALUE(XMLTYPE(y.attributes), '/Attributes/Map/entry[@key=\""+fieldName+"\"]/@value') as "+fieldName.toLowerCase();
          }
        }
        /*
         * KCS 2023-02-09 Adding in the comparison logic
         * for the comparison logic we need the compare field to be extracted into the inner query
         */
        if(Util.isNotNullOrEmpty(compareField)) {
          if("MySQL".equals(databaseType)) {
            if(compareIsBoolean) {
              log.debug("SAD-178 field "+compareField+" was identified as a boolean");
              innerQuery+= ", EXTRACTVALUE(y.attributes, '/Attributes/Map/entry[@key=\""+compareField+"\"]/value/Boolean/text()') as "+compareField.toLowerCase();
            }
            else {
              log.debug("SAD-179 field "+compareField+" NOT identified as a boolean");
              innerQuery+= ", EXTRACTVALUE(y.attributes, '/Attributes/Map/entry[@key=\""+compareField+"\"]/@value') as "+compareField.toLowerCase();
            }
          }
          else if("Microsoft SQL Server".equals(databaseType)) {
            if(compareIsBoolean) {
              log.debug("SAD-180 field "+compareField+" was identified as a boolean");
              innerQuery+= ", cast(y.attributes as xml).value('(/Attributes/Map/entry[@key=\""+compareField+"\"]/value/Boolean/text())[1]','nvarchar(max)') as "+compareField.toLowerCase();
            }
            else {
              log.debug("SAD-181 field "+compareField+" NOT identified as a boolean");
              innerQuery+= ", cast(y.attributes as xml).value('(/Attributes/Map/entry[@key=\""+compareField+"\"]/@value)[1]','nvarchar(max)') as "+compareField.toLowerCase();
            }
          }
          else if("Oracle".equals(databaseType)) {
            if(compareIsBoolean) {
              log.debug("SAD-178A field "+compareField+" was identified as a boolean");
              innerQuery+= ", EXTRACTVALUE(XMLTYPE(y.attributes), '/Attributes/Map/entry[@key=\""+compareField+"\"]/value/Boolean/text()') as "+compareField.toLowerCase();
            }
            else {
              log.debug("SAD-179A field "+compareField+" NOT identified as a boolean");
              innerQuery+= ", EXTRACTVALUE(XMLTYPE(y.attributes), '/Attributes/Map/entry[@key=\""+compareField+"\"]/@value') as "+compareField.toLowerCase();
            }
          }
        }
        /*
         * KCS 2023-02-20 Adding in the new attribute value
         */
        if(setNewValue) {
          if("MySQL".equals(databaseType)) {
            if(newAttributeIsBoolean) {
              log.debug("SAD-182 field "+newAttribute+" was identified as a boolean");
              innerQuery+= ", EXTRACTVALUE(y.attributes, '/Attributes/Map/entry[@key=\""+newAttribute+"\"]/value/Boolean/text()') as "+newAttribute.toLowerCase();
            }
            else {
              log.debug("SAD-183 field "+newAttribute+" NOT identified as a boolean");
              innerQuery+= ", EXTRACTVALUE(y.attributes, '/Attributes/Map/entry[@key=\""+newAttribute+"\"]/@value') as "+newAttribute.toLowerCase();
            }
          }
          else if("Microsoft SQL Server".equals(databaseType)) {
            if(newAttributeIsBoolean) {
              log.debug("SAD-184 field "+newAttribute+" was identified as a boolean");
              innerQuery+= ", cast(y.attributes as xml).value('(/Attributes/Map/entry[@key=\""+newAttribute+"\"]/value/Boolean/text())[1]','nvarchar(max)') as "+newAttribute.toLowerCase();
            }
            else {
              log.debug("SAD-182A field "+newAttribute+" NOT identified as a boolean");
              innerQuery+= ", cast(y.attributes as xml).value('(/Attributes/Map/entry[@key=\""+newAttribute+"\"]/@value)[1]','nvarchar(max)') as "+newAttribute.toLowerCase();
            }
          }
          else if("Oracle".equals(databaseType)) {
            if(newAttributeIsBoolean) {
              log.debug("SAD-183A field "+newAttribute+" was identified as a boolean");
              innerQuery+= ", EXTRACTVALUE(XMLTYPE(y.attributes), '/Attributes/Map/entry[@key=\""+newAttribute+"\"]/value/Boolean/text()') as "+newAttribute.toLowerCase();
            }
            else {
              log.debug("SAD-187 field "+newAttribute+" NOT identified as a boolean");
              innerQuery+= ", EXTRACTVALUE(XMLTYPE(y.attributes), '/Attributes/Map/entry[@key=\""+newAttribute+"\"]/@value') as "+newAttribute.toLowerCase();
            }
          }
        }
        /**
         * KCS 2023-03-21 Adding in IIQDisabled
         */
        String iiqDisabled="IIQDisabled";
        if("MySQL".equals(databaseType)) {
          innerQuery+= ", EXTRACTVALUE(y.attributes, '/Attributes/Map/entry[@key=\""+iiqDisabled+"\"]/value/Boolean/text()') as "+iiqDisabled.toLowerCase();
        }
        else if("Microsoft SQL Server".equals(databaseType)) {
          innerQuery+= ", cast(y.attributes as xml).value('(/Attributes/Map/entry[@key=\""+iiqDisabled+"\"]/value/Boolean/text())[1]','nvarchar(max)') as "+iiqDisabled.toLowerCase();
        }
        else if("Oracle".equals(databaseType)) {
          innerQuery+= ", EXTRACTVALUE(XMLTYPE(y.attributes), '/Attributes/Map/entry[@key=\""+iiqDisabled+"\"]/value/Boolean/text()') as "+iiqDisabled.toLowerCase();
        }
        innerQuery+= " from spt_application x, spt_link y";
        innerQuery+= " where x.name=?";
        innerQuery+= " and y.application=x.id";
        log.debug("SAD-110 innerQuery="+innerQuery);
        
        outerQuery = "select a.id as id, a.name as name, a.display_name as display_name";
        outerQuery+= ", a.type as type, a.inactive as inactive, a.correlated as correlated";
        outerQuery+=", b.native_identity as native_identity";
        if(CommonMethods.isVersionGE(sailpointVersion,sailpointPatch,"8.2","")) {
          outerQuery+=", b.iiq_disabled as iiq_disabled";
        }
        outerQuery+=", b.iiqdisabled as iiqdisabled";
        outerQuery+=", b.identity_id as identity_id";
        outerQuery+=", b."+fieldName.toLowerCase()+" as "+fieldName.toLowerCase();
        /*
         * KCS 2023-02-09 Adding in the comparison logic
         * for the comparison logic we need the compare field to be referenced in the outer query
         */
        if(Util.isNotNullOrEmpty(compareField)) {
          outerQuery+=", b."+compareField.toLowerCase()+" as "+compareField.toLowerCase();
        }
        /*
         * KCS 2023-02-20 Adding in the new attribute value
         */
        if(setNewValue) {
          outerQuery+=", b."+newAttribute.toLowerCase()+" as "+newAttribute.toLowerCase();
        }
        outerQuery+=" from spt_identity a, ("+innerQuery+") b";
        outerQuery+=" where a.id = b.identity_id";
        log.debug("SAD-111 outerQuery="+outerQuery);
        if(onlyActiveIdentities.booleanValue()) {
          outerQuery+=" and a.inactive = 0";
          log.debug("SAD-112 added filter for inactive=false");
        }
        if(onlyCorrelatedIdentities.booleanValue()) {
          outerQuery+=" and a.correlated = 1";
          log.debug("SAD-113 added filter for correlated=true");
        }
        // KCS 2023-02-09 Adding in the comparison logic
        // for the comparison logic we cannot generate histograms
        if(Util.isNotNullOrEmpty(compareField)) {
          if(ignoreNullsOnCompare) {
            if(ignoreCase) {
              // Ignore case and nulls
              // Ignore nulls means that the values must be populated
              outerQuery += " and b."+fieldName.toLowerCase()+" IS NOT NULL";
              outerQuery += " and b."+compareField.toLowerCase()+" IS NOT NULL";
              outerQuery += " and "+stringLengthFunction+"(b."+fieldName.toLowerCase()+") > 0";
              outerQuery += " and "+stringLengthFunction+"(b."+compareField.toLowerCase()+") > 0";
              if(Util.isNotNullOrEmpty(searchValue)) {
                outerQuery+=" and upper(b."+fieldName.toLowerCase()+") = upper(?)";
              }
              outerQuery += " and upper(b."+fieldName.toLowerCase()+") <> upper(b."+compareField.toLowerCase()+")";
            }
            else {
              outerQuery += " and b."+fieldName.toLowerCase()+" IS NOT NULL";
              outerQuery += " and b."+compareField.toLowerCase()+" IS NOT NULL";
              outerQuery += " and "+stringLengthFunction+"(b."+fieldName.toLowerCase()+") > 0";
              outerQuery += " and "+stringLengthFunction+"(b."+compareField.toLowerCase()+") > 0";
              if(Util.isNotNullOrEmpty(searchValue)) {
                outerQuery+=" and b."+fieldName.toLowerCase()+" = ?";
              }
              outerQuery += " and b."+fieldName.toLowerCase()+" <> b."+compareField.toLowerCase();
            }
          }
          else {
            if(ignoreCase) {
              outerQuery += " and upper(b."+fieldName.toLowerCase()+") <> upper(b."+compareField.toLowerCase()+")";
            }
            else {
              outerQuery += " and b."+fieldName.toLowerCase()+" <> b."+compareField.toLowerCase();
            }
          }
        }
        else {
          if(Util.isNullOrEmpty(searchValue) || genHistogram) {
            log.debug("SAD-114 searchValue is null or generate histogram selected, pulling all application values");
          }
          else if("ISNULL".equalsIgnoreCase(searchValue)) {
            log.debug("SAD-114 searchValue specified as ISNULL or isnull");
            outerQuery+=" and ( ( b."+fieldName.toLowerCase()+" IS NULL ) or ( "+stringLengthFunction+"(b."+fieldName.toLowerCase()+") = 0 ) )";
          }
          else if("ISNOTNULL".equalsIgnoreCase(searchValue)) {
            log.debug("SAD-114 searchValue specified as ISNOTNULL or isnotnull");
            outerQuery+=" and ( ( b."+fieldName.toLowerCase()+" IS NOT NULL ) and ( "+stringLengthFunction+"(b."+fieldName.toLowerCase()+") > 0 ) )";
          }
          else {
            if(ignoreCase) {
              outerQuery+=" and upper(b."+fieldName.toLowerCase()+") = upper(?)";
              log.debug("SAD-115 added case insensitive field name search");
            }
            else {
              outerQuery+=" and b."+fieldName.toLowerCase()+" = ?";
              log.debug("SAD-116 added case sensitive field name search");
            }
          }
        }
        outerQuery+=" order by a.name";
        log.debug("SAD-117 final outerQuery="+outerQuery);
        pstmt=sqlConnection.prepareStatement(outerQuery);
        pstmt.setString(1,appName);
        if(Util.isNullOrEmpty(searchValue) || genHistogram) {
          log.debug("SAD-114 searchValue is null or generate histogram selected, pulling all application values");
        }
        else if("ISNULL".equalsIgnoreCase(searchValue)) {
          log.debug("SAD-114 searchValue specified as ISNULL or isnull");
        }
        else if("ISNOTNULL".equalsIgnoreCase(searchValue)) {
          log.debug("SAD-114 searchValue specified as ISNOTNULL or isnotnull");
        }
        else {
          pstmt.setString(2,searchValue);
        }
        updateProgress(context, result, "SQL Query built, executing");
        queryResult=pstmt.executeQuery();
        if(queryResult!=null) {
          goodResults=true;
          updateProgress(context, result, "SQL Query successfully executed");
        }
      }
      catch(Exception qex) {
        log.error("SAD-120 "+qex.getClass().getName()+":"+qex.getMessage());
        updateProgress(context, result, "SQL Query execution failed: "+qex.getClass().getName());
      }
    }
    else {
      Filter appFilter=Filter.eq("links.application.name",appName);
      filterList.add(appFilter);
      /*
       * If onlyActiveIdentities, add that filter
       */
      if(onlyActiveIdentities.booleanValue()) {
        Filter activeFilter=Filter.eq("inactive", false);
        filterList.add(activeFilter);
        log.debug("SAD-046 added filter for inactive=false");
      }
      /*
       * If onlyCorrelatedIdentities, add that filter
       */
      if(onlyCorrelatedIdentities.booleanValue()) {
        Filter correlatedFilter=Filter.eq("correlated", true);
        filterList.add(correlatedFilter);
        log.debug("SAD-047 added filter for correlated=true");
      }
      /*
       * Check to see if multiSearch is a filter it will start with filter:
       */
      if(multiSearch.startsWith("filter:")) {
        multiSearchIsFilter=true;
        String msFilterStr=multiSearch.substring(7);
        Filter compiledFilter=Filter.compile(msFilterStr);
        filterList.add(compiledFilter);
        updateProgress(context, result, "Added custom filter");
      }
      if(filterList.size()==1) {
        qo.addFilter(appFilter);
      }
      else {
        qo.addFilter(Filter.and(filterList));
      }
      log.debug("SAD-032 Creating IncrementalObjectIterator");
      updateProgress(context, result, "Creating Object Iterator");
      IncrementalObjectIterator iter=new IncrementalObjectIterator(context,Identity.class,qo);
      if(iter!=null) {
        while(iter.hasNext()) {
          Identity identObj = (Identity)iter.next();
          identityListRaw.add(identObj.getId());
          rawcount++;
        }
      }
      updateProgress(context, result, "Raw count="+rawcount+", creating analysis maps");
    }
    String fieldValue="";
    Map<String,Integer> typeMap=new HashMap<String,Integer>();
    log.debug("SAD-033 Have iterator, creating maps");
    updateProgress(context, result, "Creating analysis maps");
    
    List<Map<String,Map<String,String>>> searchArgs=new ArrayList<Map<String,Map<String,String>>>();
    List<String> searchOps=new ArrayList<String>();
    String suffix="";
    
    if(Util.isNotNullOrEmpty(multiSearch) && (!multiSearchIsFilter)) {
      parseSearchLogic(multiSearch, searchArgs, searchOps);
      log.debug("SAD-011 searchArgs="+searchArgs.toString());
      log.debug("SAD-012 searchOps="+searchOps.toString());
      genHistogram=false;
    }
    
    if(rawcount>0) {
      for(String identId: identityListRaw) {
        Identity identObj = context.getObjectById(Identity.class,identId);
        Link link=null;
        List<Link> links = idService.getLinks(identObj,appObj);
        for(Link alink: links) {
          if(link==null) {
            link=alink;
            log.debug("SAD-035 found first link "+alink.getNativeIdentity());
          }
          else {
            log.debug("SAD-035 found multiple links including "+alink.getNativeIdentity());
          }
        }
        itercount++;
        String nativeIdentity=link.getNativeIdentity();
        log.debug("SAD-035 Found account "+nativeIdentity);
        matchedValue=false;
        addToList=false;
        if(Util.isNullOrEmpty(multiSearch) || multiSearchIsFilter) {
          /*
           * Search on a field for a value or another field
           *
           * If the field is not a String then prepend the class name to its toString value
           * Pull the fieldValue from fieldName of the link
           */
          Object fieldValueObj=link.getAttribute(fieldName);
          /*
           * To support attribute changes functionality
           */
          newAttributeValueObj=null;
          newAttributeValueStr=null;
          if(newAttribute!=null) {
            newAttributeValueObj=link.getAttribute(newAttribute);
            if(newAttributeValueObj!=null) {
              if(newAttributeValueObj instanceof String) {
                newAttributeValueStr=(String)newAttributeValueObj;
              }
              else {
                newAttributeValueStr=newAttributeValueObj.toString();
              }
            }
          }
          fieldValue="NULL";
          checkCorr=false;
          if(fieldValueObj == null) {
            fieldValueObj="NULL";
          }
          else if(fieldValueObj instanceof String) {
            fieldValue=(String)fieldValueObj;
            if(fieldValue.isEmpty()) {
              fieldValue="EMPTY STRING";
            }
          }
          else {
            fieldValue=fieldValueObj.getClass().getName()+":"+fieldValueObj.toString();
            nncount++;
          }
          log.debug("SAD-036 fieldValue="+fieldValue);
          /*
           * fieldValue is the value of the field in this Link
           * if compareField is not null then searchValue is not to be used
           */
          String localSearchValue="null";
          suffix="";
          if(Util.isNullOrEmpty(compareField)) {
            suffix="";
            if(ignoreCase) {
              if(fieldValue.equalsIgnoreCase(searchValue)) {
                count++;
                checkCorr=true;
                matchedValue=true;
                log.debug("SAD-037 matched "+searchValue);
              }
            }
            else {
              if(fieldValue.equals(searchValue)) {
                count++;
                checkCorr=true;
                matchedValue=true;
                log.debug("SAD-038 matched "+searchValue);
              }
            }
            addToList=matchedValue;
          }
          else {
            Object compareValueObj=null;
            if(compareField.startsWith("identity:")) {
              String[] compareFieldArray=compareField.split(":");
              String identityField=compareFieldArray[1];
              compareValueObj = identObj.getAttribute(identityField);
            }
            else {
              compareValueObj = link.getAttribute(compareField);
            }
            log.debug("SAD-039 compareValue="+compareValueObj);
            localSearchValue="NULL";
            if(compareValueObj==null) {
              log.debug("SAD-040 compareValue is null");
            }
            else {
              if(compareValueObj instanceof String) {
                localSearchValue=(String)compareValueObj;
              }
              else {
                localSearchValue=compareValueObj.getClass().getName()+":"+compareValueObj.toString();
              }
            }
            log.debug("SAD-041 localSearchValue="+localSearchValue);
            if(ignoreCase) {
              if(fieldValue.equalsIgnoreCase(localSearchValue)) {
                count++;
                checkCorr=true;
                matchedValue=true;
                log.debug("SAD-042 matched "+localSearchValue);
              }
              else if(compareValueObj==null) {
                // Pretend it matched just for now
                if(ignoreNullsOnCompare) {
                  matchedValue=true;
                }
              }
            }
            else {
              if(fieldValue.equals(localSearchValue)) {
                count++;
                checkCorr=true;
                matchedValue=true;
                log.debug("SAD-043 matched "+localSearchValue);
              }
              else if(compareValueObj==null) {
                // Pretend it matched just for now
                if(ignoreNullsOnCompare) {
                  matchedValue=true;
                }
              }
            }
            addToList=!matchedValue;
            suffix=" fieldValue="+fieldValue+" compareField value="+localSearchValue;
          }
          if(addToList) {
            if(genIdentityList) {
              if(identObj!=null) {
                String identValue=identObj.getName()+" = "+identObj.getDisplayName()+suffix;
                identityList.add(identValue);
              }
              else {
                log.debug("SAD-044 this account is not correlated to an Identity");
              }
            }
            if(genIdentityFilter) {
              if(identObj!=null) {
                String identValue=identObj.getName();
                identityNameList.add(identValue);
              }
            }
          }
          if(genHistogram) {
            if(histogramMap.containsKey(fieldValue)) {
              Integer countInt=histogramMap.get(fieldValue);
              countInt=1+countInt;
              histogramMap.put(fieldValue,countInt);
            }
            else {
              Integer oneInt=Integer.valueOf(1);
              histogramMap.put(fieldValue,oneInt);
            }
          }
          /*
           * So this came over from a client with a compareField not null requirement
           * but that doesn't make sense.  So removing that.  At this point
           * identObj is populated.  So it only should be activated if the
           * match is made, or if setAllNewValues is true.
           */
          if(setNewValue) {
            boolean setThisValue=false;
            if(setAllNewValues) {
              setThisValue=true;
            }
            else if(matchedValue) {
              setThisValue=true;
            }
            log.debug("SAD-091 setThisValue="+setThisValue+" setAllNewValues="+setAllNewValues+" deleteNewValue="+deleteNewValue);
            if(setThisValue) {
              if(newAttributeValueObj==null && deleteNewValue) {
                log.debug("SAD-086 the value is already null, so there is no need to change it");
              }
              else if(newAttributeValueStr!=null && newAttributeValueStr.equals(newValue)) {
                log.debug("SAD-087 the value is already the new value, so there is no need to change it");
              }
              else {
                Identity lockedIdentity = null;
                if(identObj!=null) {
                  log.debug("SAD-063 on identity "+identObj.getName()+" going to set "+newAttribute+" to "+newValue);
                  try {
                    String identObjName=identObj.getName();
                    context.decache(identObj);
                    lockedIdentity = acquireIdentityLock(context, identObjName, "Locking to remove value", 60, 0);
                    if(lockedIdentity!=null) {
                      log.debug("SAD-067 locked the identity "+identObjName);
                      if(deleteNewValue) {
                        link.setAttribute(newAttribute,null);
                        log.debug("SAD-068 cleared "+newAttribute);
                      }
                      else {
                        link.setAttribute(newAttribute,newValue);
                        log.debug("SAD-069 set "+newAttribute+" to "+newValue);
                      }
                      context.saveObject(lockedIdentity);
                      context.commitTransaction();
                      log.debug("SAD-085 saved the user "+identObjName);
                    }
                  }
                  catch (Exception ex) {
                    log.error("Trying to modify a value:"+ex.getClass().getName()+":"+ex.getMessage());
                  }
                  finally {
                    if(lockedIdentity!=null) {
                      try {
                        ObjectUtil.unlockIdentity(context,lockedIdentity);
                        context.decache(lockedIdentity);
                      }
                      catch (Exception fex) {
                        log.error("Trying to unlock an identity:"+fex.getClass().getName()+":"+fex.getMessage());
                      }
                    }
                  }
                }
              }
            }
          }
        }
        else {
          // Multi search
          checkCorr=false;
          boolean passAll=false;
          boolean passThis=false;
          boolean checkAll=false;
          List<String> compareResult=new ArrayList<String>();
          int searchNum=0;
          int numSearches=searchOps.size();
          log.debug("SAD-021 Num searches is "+numSearches);
          updateProgress(context, result, "Multi search selected");
          for(Map<String,Map<String,String>> expMap: searchArgs) {
            String op=searchOps.get(searchNum);
            log.debug("SAD-022 Search op = "+op);
            passThis=false;
            for(String fieldName: expMap.keySet()) {
              Object fieldValueObj=link.getAttribute(fieldName);
              fieldValue="null";
              if(fieldValueObj != null) {
                nncount++;
                if(fieldValueObj instanceof String) {
                  fieldValue=(String)fieldValueObj;
                }
                else {
                  fieldValue=fieldValueObj.getClass().getName()+":"+fieldValueObj.toString();
                }
                log.debug("SAD-023 Comparing fieldName = "+fieldName+" and value "+fieldValue);
                Map<String,String> compMap=expMap.get(fieldName);
                for(String val: compMap.keySet()) {
                  String opStr=compMap.get(val);
                  log.debug("SAD-024 Against value of "+val+" with operation "+opStr);
                  if("EQ".equals(opStr)) {
                    if(val.equals(fieldValue)) {
                      log.debug("SAD-025 val==fieldValue for "+fieldName+" passing this");
                      passThis=true;
                      checkCorr=true;
                    }
                  }
                  if("NE".equals(opStr)) {
                    if(!(val.equals(fieldValue))) {
                      log.debug("SAD-026 val!=fieldValue for "+fieldName+" passing this");
                      passThis=true;
                      checkCorr=true;
                    }
                  }
                }
              }
            }
            log.debug("SAD-045 passThis = "+passThis+" for fieldName="+fieldName);
            if("OR".equals(op)) {
              if(passThis)passAll=true;
              log.debug("SAD-046 op=OR so passAll = true");
            }
            else if("AND".equals(op)) {
              checkAll=true;
              log.debug("SAD-047 op=AND so checkAll = true");
            }
            if(checkAll) {
              if(searchNum==0) {
                passAll=passThis;
                log.debug("SAD-048 searchNum=0 and checkAll=true, passAll="+passAll);
              }
              else {
                if(!passThis) {
                  log.debug("SAD-049 passThis=false so passAll=false");
                  passAll=false;
                }
              }
            }
            searchNum++;
          }
          if(passAll) {
            log.debug("SAD-050 passAll=true so increment count");
            count++;
            checkCorr=true;
            if(genIdentityList) {
              if(identObj!=null) {
                String identValue=identObj.getName()+" = "+identObj.getDisplayName();
                identityList.add(identValue);
              }
            }
            if(genIdentityFilter) {
              if(identObj!=null) {
                String identValue=identObj.getName();
                identityNameList.add(identValue);
              }
            }
          }
        }
        if((itercount%100)==100) {
          log.debug("SAD-092 Attempting to update progress with value:"+String.format("%6d",itercount)+" "+nativeIdentity);
          updateProgress(context, result, String.format("%6d",itercount)+" "+nativeIdentity);
          log.debug("SAD-093 updateProgress finished");
        }
        //sboutput.append("\nLink for "+nativeIdentity+" has "+fieldName+"="+fieldValue);
        if(checkCorr) {
          log.debug("SAD-094 Check correlation selected");
          if(identObj==null) {
            sboutput.append("\nLink for "+nativeIdentity+" has null identity");
          }
          else {
            if(identObj.isCorrelated()) {
              corrcount++;
              log.debug("SAD-095 identity "+identObj.getName()+" is correlated");
            }
            else {
              noncorrcount++;
              log.debug("SAD-096 identity "+identObj.getName()+" is not correlated");
            }
            // Now check the type
            String identityType=identObj.getType();
            log.debug("SAD-097 identity "+identObj.getName()+" type is "+identityType);
            if(identityType==null) {
              identityType="null";
            }
            else if(identityType.isEmpty()) {
              identityType="Empty";
            }
            if(typeMap.containsKey(identityType)) {
              Integer nextInt=typeMap.get(identityType);
              nextInt=1+nextInt;
              typeMap.put(identityType,nextInt);
            }
            else {
              Integer oneInt=Integer.valueOf(1);
              typeMap.put(identityType,oneInt);
            }
          }
        }
        context.decache(link);
        context.decache(identObj);
      }
    }
    else if(goodResults) {
      // A good result was obtained from the database
      log.debug("SAD-121 a good result was obtained, iterating the queryResult");
      while(queryResult.next()) {
        itercount++;
        matchedValue=false;
        addToList=false;
        String identid=queryResult.getString("id");
        String identname=queryResult.getString("name");
        String identds=queryResult.getString("display_name");
        String identityType=queryResult.getString("type");
        Boolean identinactive=queryResult.getBoolean("inactive");
        Boolean identcorrelated=queryResult.getBoolean("correlated");
        String acctNativeIdentity=queryResult.getString("native_identity");
        Boolean acctDisabled=false;
        if(CommonMethods.isVersionGE(sailpointVersion,sailpointPatch,"8.2","")) {
          acctDisabled=queryResult.getBoolean("iiq_disabled");
        }
        else {
          String acctDisabledStr=queryResult.getString("iiqdisabled");
          if("true".equals(acctDisabledStr)) {
            acctDisabled=true;
          }
          else {
            acctDisabled=false;
          }
        }
        String acctFieldValue=queryResult.getString(fieldName.toLowerCase());
        log.debug("SAD-122 found identity "+identname+" : "+identds);
        if(acctFieldValue==null) {
          acctFieldValue="NULL";
        }
        else if(acctFieldValue.isEmpty()) {
          acctFieldValue="EMPTY STRING";
        }
        String acctCompareValue="";
        suffix="";
        if(Util.isNotNullOrEmpty(compareField)) {
          acctCompareValue=queryResult.getString(compareField.toLowerCase());
          if(acctCompareValue==null) {
            acctCompareValue="NULL";
          }
          else if(acctCompareValue.isEmpty()) {
            acctCompareValue="EMPTY STRING";
          }
          suffix=" fieldValue="+acctFieldValue+" compareField value="+acctCompareValue;
        }
        /*
         * if setNewValue then pull the attribute
         */
        if(setNewValue) {
          newAttributeValueStr=queryResult.getString(newAttribute.toLowerCase());
          newAttributeValueObj=newAttributeValueStr;
        }
        if(Util.isNullOrEmpty(searchValue)) {
          matchedValue=true;
          addToList=true;
          // all values pulled check for nulls
          if(Util.isNotNullOrEmpty(acctFieldValue)) nncount++;
          count++;
          if(genIdentityList) {
            String identValue=identname+" = "+identds+suffix;
            identityList.add(identValue);
          }
          if(genIdentityFilter) {
            identityNameList.add(identname);
          }
          log.debug("SAD-140 searchValue is null or empty, count="+count+" nncount="+nncount+" value="+acctFieldValue);
        }
        else if ("ISNULL".equalsIgnoreCase(searchValue)) {
          if( ("NULL".equals(acctFieldValue)) || ("EMPTY STRING".equals(acctFieldValue)) ) {
            matchedValue=true;
            addToList=true;
            count++;
            if(genIdentityList) {
              String identValue=identname+" = "+identds+suffix;
              identityList.add(identValue);
            }
            if(genIdentityFilter) {
              identityNameList.add(identname);
            }
            log.debug("SAD-142 searchValue is null or empty, count="+count+" nncount="+nncount+" value="+acctFieldValue);
          }
        }
        else if ("ISNOTNULL".equalsIgnoreCase(searchValue)) {
          if( !("NULL".equals(acctFieldValue)) && !("EMPTY STRING".equals(acctFieldValue)) ) {
            matchedValue=true;
            addToList=true;
            count++;
            nncount++;
            if(genIdentityList) {
              String identValue=identname+" = "+identds+suffix;
              identityList.add(identValue);
            }
            if(genIdentityFilter) {
              identityNameList.add(identname);
            }
            log.debug("SAD-143 searchValue is not null or empty, count="+count+" nncount="+nncount+" value="+acctFieldValue);
          }
        }
        else {
          log.debug("SAD-141 searchValue is not null or empty, value="+acctFieldValue);
          // We know here that searchValue is not null or empty so its ok to compare it
          if(Util.isNotNullOrEmpty(acctFieldValue)) {
            nncount++;
            // Now we know that acctFieldValue also is not null or empty
            if(ignoreCase) {
              if(searchValue.toLowerCase().equals(acctFieldValue.toLowerCase())) {
                count++;
                matchedValue=true;
                addToList=true;
                if(genIdentityList) {
                  String identValue=identname+" = "+identds+suffix;
                  identityList.add(identValue);
                }
                if(genIdentityFilter) {
                  identityNameList.add(identname);
                }
              }
            }
            else {
              if(searchValue.equals(acctFieldValue)) {
                count++;
                matchedValue=true;
                addToList=true;
                if(genIdentityList) {
                  String identValue=identname+" = "+identds+suffix;
                  identityList.add(identValue);
                }
                if(genIdentityFilter) {
                  identityNameList.add(identname);
                }
              }
            }
          }
        }
        if(onlyCorrelatedIdentities.booleanValue()) {
          corrcount++;
        }
        else {
          if(identcorrelated) {
            corrcount++;
          }
          else {
            noncorrcount++;
          }
        }
        if(identityType==null) {
          identityType="null";
        }
        else if(identityType.isEmpty()) {
          identityType="Empty";
        }
        if(typeMap.containsKey(identityType)) {
          Integer nextInt=typeMap.get(identityType);
          nextInt=1+nextInt;
          typeMap.put(identityType,nextInt);
        }
        else {
          Integer oneInt=Integer.valueOf(1);
          typeMap.put(identityType,oneInt);
        }
        if(genHistogram) {
          if(histogramMap.containsKey(acctFieldValue)) {
            Integer countInt=histogramMap.get(acctFieldValue);
            countInt=1+countInt;
            histogramMap.put(acctFieldValue,countInt);
          }
          else {
            Integer oneInt=Integer.valueOf(1);
            histogramMap.put(acctFieldValue,oneInt);
          }
        }
        if(setNewValue) {
          boolean setThisValue=false;
          if(setAllNewValues) {
            setThisValue=true;
          }
          else if(matchedValue) {
            setThisValue=true;
          }
          log.debug("SAD-191 setThisValue="+setThisValue+" setAllNewValues="+setAllNewValues+" deleteNewValue="+deleteNewValue);
          if(setThisValue) {
            
            if(newAttributeValueObj==null && deleteNewValue) {
              log.debug("SAD-186 the value is already null, so there is no need to change it");
            }
            else if(newAttributeValueStr!=null && newAttributeValueStr.equals(newValue)) {
              log.debug("SAD-187 the value is already the new value, so there is no need to change it");
            }
            else {
              Identity lockedIdentity = null;
              log.debug("SAD-170 on identity "+identname+" going to set "+newAttribute+" to "+newValue);
              try {
                lockedIdentity = acquireIdentityLock(context, identname, "Locking to remove value", 60, 0);
                if(lockedIdentity!=null) {
                  log.debug("SAD-171 locked the identity "+identname);
                  Link link=null;
                  List<Link> links = idService.getLinks(lockedIdentity,appObj);
                  for(Link alink: links) {
                    if(link==null) {
                      link=alink;
                      log.debug("SAD-035 found first link "+alink.getNativeIdentity());
                    }
                    else {
                      log.debug("SAD-035 found multiple links including "+alink.getNativeIdentity());
                    }
                  }
                  if(deleteNewValue) {
                    link.setAttribute(newAttribute,null);
                    log.debug("SAD-172 cleared "+newAttribute);
                  }
                  else {
                    link.setAttribute(newAttribute,newValue);
                    log.debug("SAD-173 set "+newAttribute+" to "+newValue);
                  }
                  context.saveObject(lockedIdentity);
                  context.commitTransaction();
                  log.debug("SAD-188 saved the user "+identname);
                }
              }
              catch (Exception ex) {
                log.error("SAD-189 Trying to modify a value:"+ex.getClass().getName()+":"+ex.getMessage());
              }
              finally {
                if(lockedIdentity!=null) {
                  try {
                    ObjectUtil.unlockIdentity(context,lockedIdentity);
                    context.decache(lockedIdentity);
                  }
                  catch (Exception fex) {
                    log.error("SAD-190 Trying to unlock an identity:"+fex.getClass().getName()+":"+fex.getMessage());
                  }
                }
              }
            }
          }
        }
      }
      if((itercount%100)==0) {
        updateProgress(context, result, "Analyzed "+(Integer.valueOf(itercount)).toString()+" accounts");
      }
    }
    log.debug("SAD-125 Out of "+itercount+" links, not null= "+nncount);
    sboutput.append("\nOut of "+itercount+" links, not null= "+nncount);
    if(Util.isNotNullOrEmpty(fieldName) && Util.isNotNullOrEmpty(searchValue)) {
      log.debug("SAD-126 Count of "+fieldName+"="+searchValue+", count = "+count);
      sboutput.append("\nCount of "+fieldName+"="+searchValue+", count = "+count);
    }
    else if(Util.isNotNullOrEmpty(multiSearch) && (!multiSearchIsFilter)) {
      log.debug("SAD-127 Count of multi valued search = "+count);
      sboutput.append("\nCount of multi valued search = "+count);
    }
    log.debug("SAD-128 Of these, "+corrcount+" are correlated identities");
    sboutput.append("\nOf these, "+corrcount+" are correlated identities");
    log.debug("SAD-129 and "+noncorrcount+" are uncorrelated identities");
    sboutput.append("\nand "+noncorrcount+" are uncorrelated identities");
    log.debug("SAD-130 Identity Type counts:");
    sboutput.append("\nIdentity Type counts:");
    for(String typeStr:typeMap.keySet()) {
      log.debug("SAD-131 "+String.format("%20s",typeStr)+" count = "+typeMap.get(typeStr));
      sboutput.append("\n"+String.format("%20s",typeStr)+" count = "+typeMap.get(typeStr));
    }
    if(genHistogram) {
      updateProgress(context, result, "Generating histogram sort: "+histoSort);
      log.debug("SAD-132 Histogram of "+fieldName+" values");
      sboutput.append("\n\nHistogram of "+fieldName+" values");
      log.debug("SAD-162 creating the sorted maps");
      SortedSet<String> valueSet=new TreeSet<String>();
      SortedSet<Integer> revcountSet=new TreeSet<Integer>(Collections.reverseOrder());
      SortedSet<String> countvalueSet=new TreeSet<String>();
      int maxKeyLen=10;
      for(String histKey: histogramMap.keySet()) {
        Integer histValue=histogramMap.get(histKey);
        log.debug("SAD-163 Adding "+histKey+" to valueSet");
        valueSet.add(histKey);
        log.debug("SAD-164 Adding "+histValue+" to revcountSet");
        revcountSet.add(histValue);
        String countvalue=String.format("%d",histValue.intValue());
        countvalue="000000000"+countvalue;
        countvalue=countvalue.substring(countvalue.length()-9);
        log.debug("SAD-165 Adding "+countvalue+"-"+histKey+" to countvalueSet");
        countvalueSet.add(countvalue+"-"+histKey);
        int keyLen=histKey.length();
        if(keyLen>maxKeyLen)maxKeyLen=keyLen;
      }
      String keyFormat="%"+String.format("%d",maxKeyLen)+"s";
      log.debug("SAD-166 maxKeyLen="+maxKeyLen+" keyFormat="+keyFormat);
      if("value".equalsIgnoreCase(histoSort)) {
        log.debug("SAD-167 histoSort=value, sorting on value using valueSet");
        for(String histKey: valueSet) {
          Integer histValue=histogramMap.get(histKey);
          log.debug("SAD-133 "+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
          sboutput.append("\n"+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
        }
      }
      else if("asc".equalsIgnoreCase(histoSort)) {
        log.debug("SAD-168 histoSort=asc, sorting on count using countvalueSet");
        for(String countvalueKey: countvalueSet) {
          Integer histValue=Integer.valueOf(countvalueKey.substring(0,9));
          String histKey=countvalueKey.substring(10);
          log.debug("SAD-133 "+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
          sboutput.append("\n"+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
        }
      }
      else if("desc".equalsIgnoreCase(histoSort)) {
        log.debug("SAD-169 histoSort=desc, sorting descending count using revcountSet");
        for(Integer revcountKey: revcountSet) {
          String countvalue=String.format("%d",revcountKey.intValue());
          countvalue="000000000"+countvalue;
          countvalue=countvalue.substring(countvalue.length()-9);
          for(String countvalueKey: countvalueSet) {
            if(countvalueKey.startsWith(countvalue)) {
              Integer histValue=Integer.valueOf(countvalueKey.substring(0,9));
              String histKey=countvalueKey.substring(10);
              log.debug("SAD-133 "+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
              sboutput.append("\n"+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
            }
          }
        }
      }
      else {
        for(String histKey: histogramMap.keySet()) {
          Integer histValue=histogramMap.get(histKey);
          log.debug("SAD-133 "+String.format("%20s",histKey)+":"+String.format("%5d",histValue.intValue()));
          sboutput.append("\n"+String.format("%20s",histKey)+":"+String.format("%5d",histValue.intValue()));
        }
      }
    }
    if(genIdentityList) {
      updateProgress(context, result, "Generating identity list");
      if(Util.isNullOrEmpty(compareField)) {
        log.debug("SAD-134 List of matched identities");
        sboutput.append("\n\nList of matched identities");
      }
      else {
        log.debug("SAD-135 List of identities not matching");
        sboutput.append("\n\nList of identities not matching");
      }
      int listCount=0;
      int listLength=identityList.size();
      // Analyze listModuloStr
      listModuloStr=listModuloStr.trim();
      listModulo=1;
      // If it ends with a % sign
      if (listModuloStr.endsWith("%")) {
        log.debug("SAD-138 found listModuloStr ends with %");
        // User wants a percent of the users
        double hundred=100.0;
        double listPct=100.0;
        int llmslen=listModuloStr.length();
        // This should be the string minus the %
        String listModuloStrNP=listModuloStr.substring(0,llmslen-1);
        log.debug("SAD-139 listModuloStrNP="+listModuloStrNP);
        try {
          Double listModuloPct=Double.valueOf(listModuloStrNP);
          listPct = listModuloPct.doubleValue();
        }
        catch (Exception exp) {}
        // Example list length is 5407 users
        // User is asking for 15%
        // listLengthK = 54.07
        // listPct = 15.0
        // listModulo should be 54.07 * 15.0 = 811.05
        listModulo = (int)((hundred / listPct)+0.01);
      }
      else {
        try {
          listModulo=Integer.valueOf(listModuloStr);
        }
        catch (Exception exi) {}
      }
      if(identityList.isEmpty()) {
        log.debug("SAD-138 identityList is empty, proceeding");
        sboutput.append("\nNo matches");
      }
      else {
        for(String identValue: identityList) {
          boolean shouldPrint=false;
          listCount++;
          if(listModulo <= 1) {
            shouldPrint=true;
          }
          else {
            if((listCount % listModulo) == 0) {
              shouldPrint=true;
            }
          }
          if(shouldPrint) {
            log.debug("SAD-136 "+identValue);
            sboutput.append("\n"+identValue);
          }
        }
      }
    }
    if(genIdentityFilter) {
      log.debug("SAD-110 filter of names");
      sboutput.append("\n\nName filter for use in Identity Refresh task:\n");
      sboutput.append("\nname.in({");
      int numIdents=identityNameList.size();
      int iname=0;
      String dquote="\"";
      for(String identValue: identityNameList) {
        if(iname>0) {
          sboutput.append(",");
        }
        sboutput.append(dquote+identValue+dquote);
        iname++;
        if( ((iname%20)==0) && (iname!=numIdents) ) {
          sboutput.append("\n");
        }
      }
      sboutput.append("})\n");
    }
    log.debug("SAD-137 processed");
    sboutput.append("\n\nprocessed");
    updateProgress(context, result, "Wrapping up");
    /*
     * Display the results
     */
    log.debug("SAD-111 totalCount="+totalCount);
    result.setAttribute("totalCount", count);
    result.setAttribute("resultString", sboutput.toString());
    taskResult.setCompletionStatus(TaskResult.CompletionStatus.Success);
    taskResult.addMessage(new Message(Message.Type.Info,"Processed"));
    log.debug("SAD-112 exiting");
    return;
  }
  
  private boolean parseSearchLogic(String multiSearchInput, List<Map<String,Map<String,String>>>searchArgs, List<String>searchOps) {
    String workingInput=multiSearchInput;
    boolean foundError=false;
    int flogLen=0;
    String opStr="";
    while(workingInput.length() > 0) {
      // Find any logic symbol AND OR && ||
      int flogIndex=-1;
      int candIndex=workingInput.indexOf("AND");
      int landIndex=workingInput.indexOf("&&");
      int corIndex=workingInput.indexOf("OR");
      int lorIndex=workingInput.indexOf("||");
      if(candIndex>0 &&  flogIndex<0) {
        flogIndex=candIndex;
        flogLen=3;
        opStr="AND";
        log.debug("SAD-051 Found "+opStr+" at "+flogIndex);
      }
      if(landIndex>0 &&  flogIndex<0) {
        flogIndex=landIndex;
        flogLen=2;
        opStr="AND";
        log.debug("SAD-052 Found "+opStr+" at "+flogIndex);
      }
      if(corIndex>0 &&  flogIndex<0) {
        flogIndex=corIndex;
        flogLen=2;
        opStr="OR";
        log.debug("SAD-053 Found "+opStr+" at "+flogIndex);
      }
      if(lorIndex>0 &&  flogIndex<0) {
        flogIndex=lorIndex;
        flogLen=2;
        opStr="OR";
        log.debug("SAD-054 Found "+opStr+" at "+flogIndex);
      }
      if(candIndex>0 &&  candIndex<flogIndex) {
        flogIndex=candIndex;
        flogLen=3;
        opStr="AND";
        log.debug("SAD-055 Found "+opStr+" at "+flogIndex);
      }
      if(landIndex>0 &&  landIndex<flogIndex) {
        flogIndex=landIndex;
        flogLen=2;
        opStr="AND";
        log.debug("SAD-056 Found "+opStr+" at "+flogIndex);
      }
      if(corIndex>0 &&  corIndex<flogIndex) {
        flogIndex=corIndex;
        flogLen=2;
        opStr="OR";
        log.debug("SAD-057 Found "+opStr+" at "+flogIndex);
      }
      if(lorIndex>0 &&  lorIndex<flogIndex) {
        flogIndex=lorIndex;
        flogLen=2;
        opStr="OR";
        log.debug("SAD-058 Found "+opStr+" at "+flogIndex);
      }
      if(flogIndex<0) {
        // there is no logic symbol
        String logicalExpression=workingInput;
        log.debug("SAD-061 Found logical expression "+logicalExpression);
        foundError=addLogicalExpression(logicalExpression,searchArgs,"None",searchOps);
        workingInput="";
      }
      else {
        String logicalExpression=workingInput.substring(0,flogIndex);
        log.debug("SAD-063 Found logical expression "+logicalExpression);
        foundError=addLogicalExpression(logicalExpression,searchArgs,opStr,searchOps);
        workingInput=workingInput.substring(flogIndex+flogLen);
        log.debug("SAD-064 workingInput is now "+workingInput);
      }
      if(foundError)break;
    }
    return foundError;
  }
  
  private boolean addLogicalExpression(String exp, List<Map<String,Map<String,String>>>searchArgs, String op, List<String>searchOps) {
    int eqIndex=exp.indexOf("==");
    int neIndex=exp.indexOf("!=");
    int ltIndex=exp.indexOf("<");
    int leIndex=exp.indexOf("<=");
    int gtIndex=exp.indexOf(">");
    int geIndex=exp.indexOf(">=");
    int nnIndex=exp.indexOf("notnull");
    int nuIndex=exp.indexOf("isnull");
    int opIndex=-1;
    int opLen=0;
    
    String opStr="";
    if(eqIndex>0 && opIndex<0) {
      opIndex=eqIndex;
      opStr="EQ";
      opLen=2;
      log.debug("SAD-071 found opStr "+opStr+" at "+opIndex);
    }
    if(neIndex>0 && opIndex<0) {
      opIndex=neIndex;
      opStr="NE";
      opLen=2;
      log.debug("SAD-072 found opStr "+opStr+" at "+opIndex);
    }
    if(ltIndex>0 && leIndex<0 && opIndex<0) {
      opIndex=ltIndex;
      opStr="LT";
      opLen=1;
      log.debug("SAD-073 found opStr "+opStr+" at "+opIndex);
    }
    if(leIndex>0 && opIndex<0) {
      opIndex=leIndex;
      opStr="LE";
      opLen=2;
      log.debug("SAD-074 found opStr "+opStr+" at "+opIndex);
    }
    if(gtIndex>0 && geIndex<0 && opIndex<0) {
      opIndex=gtIndex;
      opStr="GT";
      opLen=1;
      log.debug("SAD-075 found opStr "+opStr+" at "+opIndex);
    }
    if(geIndex>0 && opIndex<0) {
      opIndex=geIndex;
      opStr="GE";
      opLen=2;
      log.debug("SAD-076 found opStr "+opStr+" at "+opIndex);
    }
    if(nnIndex>0 && opIndex<0) {
      opIndex=nnIndex;
      opStr="NN";
      opLen=7;
      log.debug("SAD-077 found opStr "+opStr+" at "+opIndex);
    }
    if(nuIndex>0 && opIndex<0) {
      opIndex=nuIndex;
      opStr="NU";
      opLen=6;
      log.debug("SAD-078 found opStr "+opStr+" at "+opIndex);
    }
    if(opIndex<0) {
      log.error("SAD-079 Did not find a logic operation in this expression: "+exp);
      return true;
    }
    else {
      log.debug("SAD-080 Found logical expression "+opStr+" at character "+opIndex+" in "+exp);
    }
    String arg=exp.substring(0,opIndex).trim();
    log.debug("SAD-081 Found argument "+arg);
    String val="NULL";
    if(opLen<=2) {
      val=exp.substring(opIndex+opLen).trim();
    }
    log.debug("SAD-082 Found expression "+arg+" "+opStr+" "+val);
    Map<String,String> compMap=new HashMap<String,String>();
    compMap.put(val,opStr);
    log.debug("SAD-083 Created compMap="+compMap.toString());
    Map<String,Map<String,String>>expMap=new HashMap<String,Map<String,String>>();
    expMap.put(arg,compMap);
    log.debug("SAD-084 Created expMap="+expMap.toString());
    searchArgs.add(expMap);
    searchOps.add(op);
    return false;
  }
  
  // From https://community.sailpoint.com/t5/Technical-White-Papers/BSDG-20-Locking-Identity-Objects-for-Modification/ta-p/76456
  // A helper function that attempts to acquire a lock on an Identity.  It will
  // wait for 'waitSecs' for any existing locks to go away before giving up an
  // attempt, and it will re-attempt 'retryTimes' before giving up entirely.
  // On a successful lock it will return a valid sailpoint.object.Identity
  // reference.  If it fails to acquire a lock then it will return a null Identity
  // reference to the caller and it will display various messages in the log file.
  // After the last re-attempt it will give up and log a full stack trace
  // allowing system administrators to review the issue.
  // The 'lockName' argument is an option string that can describe the process
  // that is acquiring the lock on the Identity.  If null or an empty string is
  // passed for this then the host + thread name will be substitued in for the
  // value of 'lockName'.
  // Note: this can be copied and pasted into site-specific code or places into
  // rule libraries as necessary.
  private Identity acquireIdentityLock (SailPointContext context, String identityId, String lockName, int waitSecs, int retryTimes) throws Exception
    {
    // Sanity check the arguments passed in, to prevent irrationally short calls.
    if (retryTimes <= 0) {
      retryTimes = 1;
    }
    if (waitSecs <= 0) {
      waitSecs = 5;
    }

    // Make sure we've been asked to lock a valid Identity.
    Identity idToLock = context.getObjectById(Identity.class, identityId);
    if (null == idToLock) {
      idToLock = context.getObjectByName(Identity.class, identityId);
    }
    if (null == idToLock) {
      log.error("Could not find an Identity to lock matching: [" + identityId + "]");
      return null;
    }

    int numLockRetries = 0;
    Identity lockedId = null;

    // If no lock name was passed in then create one that's descriptive of
    // the host and thread that acquired the lock came from.
    if ( (lockName instanceof String) && (0 == lockName.length()) ) {
      lockName = null;
    }
    if (lockName == null) {
      String hostName = java.net.InetAddress.getLocalHost().getHostName();
      long threadId = Thread.currentThread().getId();
      String threadName = Thread.currentThread().getName();
      lockName = "host:[" + hostName + "], threadName:[" + threadId + "], thread:[" + threadName +"]";
    }

    while ((lockedId == null) && (numLockRetries < retryTimes)) {

      try {

        // Attempt to acquire a lock in the object.
        lockedId = ObjectUtil.lockIdentity(context, identityId, waitSecs);

      } catch (sailpoint.api.ObjectAlreadyLockedException ex) {

        // Let's see who's got this object currently locked.
        String lockString = idToLock.getLock();
        if ((null == lockString) || (0 == lockString.length())) {
          lockString = "unspecified";
        }

        // Log the stack trace on the final attempt to retry.
        if (numLockRetries == (retryTimes - 1)) {
          String eMsg = "Failed to acquire lock on Identity [" + identityId + "], lock held by: [" + lockString + "]";
          log.error(eMsg, ex);
        } else {
          String wMsg = "Timeout acquiring lock on Identity [" + identityId + "], lock held by: [" + lockString + "], retrying.";
          log.warn(wMsg);
        }
      }
      numLockRetries++;
    }
    return lockedId;
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
