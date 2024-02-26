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

import sailpoint.api.SailPointFactory;
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
import sailpoint.object.Configuration;
import sailpoint.object.Dictionary;
import sailpoint.object.DictionaryTerm;
import sailpoint.object.Filter;
import sailpoint.object.Identity;
import sailpoint.object.IdentityTypeDefinition;
import sailpoint.object.Link;
import sailpoint.object.ObjectAttribute;
import sailpoint.object.ObjectConfig;
import sailpoint.object.Profile;
import sailpoint.object.QueryOptions;
import sailpoint.object.Resolver;
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
public class SearchIdentityData extends BasePluginTaskExecutor {
  private static final String PLUGIN_NAME = "MCSpoofingPlugin";
  
  private static Log log = LogFactory.getLog(SearchIdentityData.class);
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
    log.debug("SID-000 Version "+sailpointVersion+" patch level "+sailpointPatch);
    String nowDateNoTimeStr=sdfnotime.format(now);
    try {
      nowNoTime=sdfnotime.parse(nowDateNoTimeStr);
    }
    catch (Exception et) {}
    oldestDate=nowNoTime.getTime();
    sboutput.append("SearchIdentityData started at "+sdfout.format(now));
    log.debug("SID-001 Starting Search Account Data");
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
    appName="IIQ";
    /*
    if(args.containsKey("appName")) {
      appName=args.getString("appName");
      log.debug("SID-002 Application name:"+appName);
    }
    else {
      terminateMissingInput=true;
      log.error("SID-002 Application name is missing");
    }
    */
    if(args.containsKey("fieldName")) {
      fieldName=args.getString("fieldName");
      if(fieldName.toLowerCase().startsWith("boolean:")) {
        fieldIsBoolean=true;
        fieldName=fieldName.substring(8);
      }
      log.debug("SID-003 Field name:"+fieldName);
    }
    else {
      terminateMissingInput=true;
      log.debug("SID-003 Field name is missing");
    }
    
    if(args.containsKey("searchValue")) {
      searchValue=args.getString("searchValue");
      log.debug("SID-004 Search value:"+searchValue);
    }
    
    ignoreCase=args.getBoolean("ignoreCase");
    log.debug("SID-005 Ignore Case:"+ignoreCase);
    
    genHistogram=args.getBoolean("genHistogram");
    log.debug("SID-006 Generate Histotram:"+genHistogram);
    
    if(args.containsKey("multiSearch")) {
      multiSearch=args.getString("multiSearch");
      log.debug("SID-007 multiSearch:"+multiSearch);
    }
    
    genIdentityList=args.getBoolean("genIdentityList");
    log.debug("SID-008 Generate Identity List:"+genIdentityList);
  
  /*
   * List modulo is new to only print every N'th identity
   */
    if(args.containsKey("listModulo")) {
      listModuloStr=args.getString("listModulo");
      log.debug("SID-009 List Modulo specified as "+listModuloStr);
    }
    
    genIdentityFilter=args.getBoolean("genIdentityFilter");
    log.debug("SID-008 Generate Identity Filter:"+genIdentityFilter);
    
    if(args.containsKey("compareField")) {
      compareField=args.getString("compareField");
      if(compareField.toLowerCase().startsWith("boolean:")) {
        compareIsBoolean=true;
        compareField=compareField.substring(8);
      }
      log.debug("SID-027 Compare Field="+compareField);
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
      log.debug("SID-161 Histogram Sort="+histoSort);
    }
    
    ignoreNullsOnCompare=args.getBoolean("ignoreNulls");
    log.debug("SID-059 Ignore Nulls on Compare:"+ignoreNullsOnCompare);

    /*
   * newAttribute means to set the value for an attribute
   */
    if(args.containsKey("newAttribute")) {
      newAttribute=args.getString("newAttribute");
      if(newAttribute.toLowerCase().startsWith("boolean:")) {
        newAttributeIsBoolean=true;
        newAttribute=newAttribute.substring(8);
      }
      log.debug("SID-064 Set values on this attribute:"+newAttribute);
      if(args.containsKey("newValue")) {
        newValue=args.getString("newValue");
        log.debug("SID-060 Set values to this value:"+newValue);
      }
    }
    if(newValue!=null && newAttribute!=null) {
      setNewValue=true;
      if(newValue.startsWith("all:")) {
        setAllNewValues=true;
        newValue=newValue.substring(4);
        log.debug("SID-066 Setting all values of "+newAttribute+" to "+newValue);
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
    log.debug("SID-007 Unix Origin="+unixOrigin);
    if ((!terminate) && (!terminateMissingInput)) {
      log.debug("SID-008 initializing from inputs");
    }
    else {
      log.error("SID-009 Terminate chosen or missing inputs");
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
        sboutput.append("\nSearching "+appName+" for "+fieldName+" = "+searchValue);
        log.debug("SID-010 Searching "+appName+" for "+fieldName+" = "+searchValue);
        updateProgress(context, result, "Searching "+appName+" for "+fieldName+" = "+searchValue);
      }
      else {
        sboutput.append("\nSearching "+appName+" for "+fieldName+" != "+compareField);
        log.debug("SID-029 Searching "+appName+" for "+fieldName+" != "+compareField);
        updateProgress(context, result, "Searching "+appName+" with "+fieldName+" != "+compareField);
      }
      if(ignoreCase) {
        sboutput.append("\n ignoring case");
      }
    }
    log.debug("SID-030 Creating histogramMap");
    Map<String,Integer> histogramMap=new HashMap<String,Integer>();
    /*
     * Get the object config - identity object
     */
    SailPointContext neuCtx = SailPointFactory.createPrivateContext();
    ObjectConfig fromObj=neuCtx.getObjectByName(ObjectConfig.class,"Identity");
    ObjectConfig identityConfig=(ObjectConfig)fromObj.derive((Resolver)neuCtx);
    identityConfig.setName("ClonedIdentity");
    Map<String,IdentityTypeDefinition> identityTypes=identityConfig.getIdentityTypesMap();
    Map<String,ObjectAttribute> objMap=identityConfig.getObjectAttributeMap();
    ObjectAttribute fieldNameAttribute=null;
    ObjectAttribute compareFieldAttribute=null;
    ObjectAttribute newAttributeAttribute=null;
    /*
     * Need to determine the names of the fields in the database.
     */
    String fieldNameDBName=fieldName;
    String compareFieldDBName=compareField;
    String newAttributeDBName=newAttribute;
    //
    if(objMap.containsKey(fieldName)) {
      log.debug("SID-201 Found "+fieldName+" in the attribute map");
      fieldNameAttribute=objMap.get(fieldName);
      log.debug("SID-202 isNamedColumn="+fieldNameAttribute.isNamedColumn());
      log.debug("SID-203 isExtended="+fieldNameAttribute.isExtended());
      if(fieldNameAttribute.isNamedColumn()) {
        fieldNameDBName=generateDBFieldFromIdField(fieldName);
      }
      else if(fieldNameAttribute.isExtended()) {
        int extendedNumber=fieldNameAttribute.getExtendedNumber();
        log.debug("SID-204 extendedNumber="+extendedNumber);
        fieldNameDBName="extended"+(Integer.valueOf(extendedNumber)).toString().trim();
      }
    }
    else {
      log.error("SID-205 fieldName not found in ObjectConfig-Identity");
      taskResult.setCompletionStatus(TaskResult.CompletionStatus.Warning);
      taskResult.addMessage(new Message(Message.Type.Info,"Search field not found in ObjectConfig-Identity"));
      return;
    }
    if(Util.isNotNullOrEmpty(compareField)) {
      if(objMap.containsKey(compareField)) {
        log.debug("SID-211 Found "+compareField+" in the attribute map");
        compareFieldAttribute=objMap.get(compareField);
        log.debug("SID-212 isNamedColumn="+compareFieldAttribute.isNamedColumn());
        log.debug("SID-213 isExtended="+compareFieldAttribute.isExtended());
        if(compareFieldAttribute.isNamedColumn()) {
          compareFieldDBName=generateDBFieldFromIdField(compareField);
        }
        else if(compareFieldAttribute.isExtended()) {
          int extendedNumber=compareFieldAttribute.getExtendedNumber();
          log.debug("SID-214 extendedNumber="+extendedNumber);
          compareFieldDBName="extended"+(Integer.valueOf(extendedNumber)).toString().trim();
        }
      }
      else {
        log.error("SID-215 compareField not found in ObjectConfig-Identity");
        taskResult.setCompletionStatus(TaskResult.CompletionStatus.Warning);
        taskResult.addMessage(new Message(Message.Type.Info,"Compare field not found in ObjectConfig-Identity"));
        return;
      }
    }
    if(Util.isNotNullOrEmpty(newAttribute)) {
      if(objMap.containsKey(newAttribute)) {
        log.debug("SID-216 Found "+newAttribute+" in the attribute map");
        newAttributeAttribute=objMap.get(newAttribute);
        log.debug("SID-217 isNamedColumn="+newAttributeAttribute.isNamedColumn());
        log.debug("SID-218 isExtended="+newAttributeAttribute.isExtended());
        if(newAttributeAttribute.isNamedColumn()) {
          newAttributeDBName=generateDBFieldFromIdField(newAttribute);
        }
        else if(newAttributeAttribute.isExtended()) {
          int extendedNumber=newAttributeAttribute.getExtendedNumber();
          log.debug("SID-219 extendedNumber="+extendedNumber);
          newAttributeDBName="extended"+(Integer.valueOf(extendedNumber)).toString().trim();
        }
      }
      else {
        log.error("SID-220 newAttribute not found in ObjectConfig-Identity");
        taskResult.setCompletionStatus(TaskResult.CompletionStatus.Warning);
        taskResult.addMessage(new Message(Message.Type.Info,"New Attribute field not found in ObjectConfig-Identity"));
        return;
      }
    }
    /*
     * Create the query options
     */
    log.debug("SID-031 Creating QueryOptions and Filter");
    QueryOptions qo=new QueryOptions();
    List<Filter> filterList=new ArrayList<Filter>();
    int itercount=0;
    int nncount=0;
    int corrcount=0;
    int noncorrcount=0;
    int count=0;
    int rawcount=0;
    log.debug("SID-034 initializing identityList");
    /*
     * identityList is a list of identity name and display name for matches
     * identityListRaw is a list of identity IDs for all accounts on an application
     * this is why the SQL is better as it limits the number of records pulled.
     */
    List<String> identityList=new ArrayList<String>();
    List<String> identityNameList=new ArrayList<String>();
    List<String> identityListRaw=new ArrayList<String>();
    //IdentityService idService=new IdentityService(context);
    //Application appObj=context.getObjectByName(Application.class,appName);
    java.sql.Connection sqlConnection=null;
    PreparedStatement pstmt=null;
    String outerQuery=null;
    ResultSet queryResult=null;
    boolean goodResults=false;
    if(useSQL) {
      sqlConnection=context.getJdbcConnection();
      DatabaseMetaData dbmd=sqlConnection.getMetaData();
      databaseType=dbmd.getDatabaseProductName();
      log.debug("SID-160 Database product name="+databaseType);
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
       * No inner query needed
       *
       * The outer query is:
       * select a.id, a.name, a.displayName, a.type, a.inactive, a.correlated
       * , a.<fieldName>
       * from spt_identity a
       * (optionally)
       * where
       * and a.type = 
       * and a.inactive = 
       * and a.correlated = 
       */
      try {
        outerQuery = "select a.id as id, a.name as name, a.display_name as display_name";
        outerQuery+= ", a.type as type, a.inactive as inactive, a.correlated as correlated";
        if(fieldNameAttribute.isNamedColumn()) {
          log.debug("SID-221 Adding "+fieldName+" from named column as "+fieldNameDBName);
          outerQuery+=", a."+fieldNameDBName+" as "+fieldName.toLowerCase();
        }
        else if(fieldNameAttribute.isExtended()) {
          int extendedNumber=fieldNameAttribute.getExtendedNumber();
          log.debug("SID-222 Adding "+fieldName+" as extended"+extendedNumber);
          outerQuery+=", a."+fieldNameDBName+" as "+fieldName.toLowerCase();
        }
        else {
          log.debug("SID-223 Adding "+fieldName+" from the XML");
          if("MySQL".equals(databaseType)) {
            if(fieldIsBoolean) {
              log.debug("SID-224 field "+fieldName+" was identified as a boolean");
              outerQuery+= ", EXTRACTVALUE(a.attributes, '/Attributes/Map/entry[@key=\""+fieldName+"\"]/value/Boolean/text()') as "+fieldName.toLowerCase();
            }
            else {
              log.debug("SID-225 field "+fieldName+" NOT identified as a boolean");
              outerQuery+= ", EXTRACTVALUE(a.attributes, '/Attributes/Map/entry[@key=\""+fieldName+"\"]/@value') as "+fieldName.toLowerCase();
            }
          }
          else if("Microsoft SQL Server".equals(databaseType)) {
            if(fieldIsBoolean) {
              log.debug("SID-226 field "+fieldName+" was identified as a boolean");
              outerQuery+= ", cast(a.attributes as xml).value('(/Attributes/Map/entry[@key=\""+fieldName+"\"]/value/Boolean/text())[1]','nvarchar(max)') as "+fieldName.toLowerCase();
            } 
            else {
              log.debug("SID-227 field "+fieldName+" NOT identified as a boolean");
              outerQuery+= ", cast(a.attributes as xml).value('(/Attributes/Map/entry[@key=\""+fieldName+"\"]/@value)[1]','nvarchar(max)') as "+fieldName.toLowerCase();
            }
          }
          else if("Oracle".equals(databaseType)) {
            if(fieldIsBoolean) {
              log.debug("SID-228 field "+fieldName+" was identified as a boolean");
              outerQuery+= ", EXTRACTVALUE(XMLTYPE(a.attributes), '/Attributes/Map/entry[@key=\""+fieldName+"\"]/value/Boolean/text()') as "+fieldName.toLowerCase();
            }
            else {
              log.debug("SID-229 field "+fieldName+" NOT identified as a boolean");
              outerQuery+= ", EXTRACTVALUE(XMLTYPE(a.attributes), '/Attributes/Map/entry[@key=\""+fieldName+"\"]/@value') as "+fieldName.toLowerCase();
            }
          }
        }
        /*
         * KCS 2023-02-09 Adding in the comparison logic
         * for the comparison logic we need the compare field to be referenced in the outer query
         */
        if(Util.isNotNullOrEmpty(compareField)) {
          if(compareFieldAttribute.isNamedColumn()) {
            log.debug("SID-231 Adding "+compareField+" from named column as "+compareFieldDBName);
            outerQuery+=", a."+compareFieldDBName+" as "+compareField.toLowerCase();
          }
          else if(compareFieldAttribute.isExtended()) {
            int extendedNumber=compareFieldAttribute.getExtendedNumber();
            log.debug("SID-232 Adding "+compareField+" as extended"+extendedNumber);
            outerQuery+=", a."+compareFieldDBName+" as "+compareField.toLowerCase();
          }
          else {
            log.debug("SID-233 Adding "+compareField+" from the XML");
            if("MySQL".equals(databaseType)) {
              if(fieldIsBoolean) {
                log.debug("SID-234 field "+compareField+" was identified as a boolean");
                outerQuery+= ", EXTRACTVALUE(a.attributes, '/Attributes/Map/entry[@key=\""+compareField+"\"]/value/Boolean/text()') as "+compareField.toLowerCase();
              }
              else {
                log.debug("SID-235 field "+compareField+" NOT identified as a boolean");
                outerQuery+= ", EXTRACTVALUE(a.attributes, '/Attributes/Map/entry[@key=\""+compareField+"\"]/@value') as "+compareField.toLowerCase();
              }
            }
            else if("Microsoft SQL Server".equals(databaseType)) {
              if(fieldIsBoolean) {
                log.debug("SID-236 field "+compareField+" was identified as a boolean");
                outerQuery+= ", cast(a.attributes as xml).value('(/Attributes/Map/entry[@key=\""+compareField+"\"]/value/Boolean/text())[1]','nvarchar(max)') as "+compareField.toLowerCase();
              } 
              else {
                log.debug("SID-237 field "+compareField+" NOT identified as a boolean");
                outerQuery+= ", cast(a.attributes as xml).value('(/Attributes/Map/entry[@key=\""+compareField+"\"]/@value)[1]','nvarchar(max)') as "+compareField.toLowerCase();
              }
            }
            else if("Oracle".equals(databaseType)) {
              if(fieldIsBoolean) {
                log.debug("SID-238 field "+compareField+" was identified as a boolean");
                outerQuery+= ", EXTRACTVALUE(XMLTYPE(a.attributes), '/Attributes/Map/entry[@key=\""+compareField+"\"]/value/Boolean/text()') as "+compareField.toLowerCase();
              }
              else {
                log.debug("SID-239 field "+compareField+" NOT identified as a boolean");
                outerQuery+= ", EXTRACTVALUE(XMLTYPE(a.attributes), '/Attributes/Map/entry[@key=\""+compareField+"\"]/@value') as "+compareField.toLowerCase();
              }
            }
          }
        }
        /*
         * KCS 2023-02-20 Adding in the new attribute value
         */
        if(setNewValue && Util.isNotNullOrEmpty(newAttribute)) {
          if(newAttributeAttribute.isNamedColumn()) {
            log.debug("SID-241 Adding "+newAttribute+" from named column as "+newAttributeDBName);
            outerQuery+=", a."+newAttributeDBName+" as "+newAttribute.toLowerCase();
          }
          else if(newAttributeAttribute.isExtended()) {
            int extendedNumber=newAttributeAttribute.getExtendedNumber();
            log.debug("SID-242 Adding "+newAttribute+" as extended"+extendedNumber);
            outerQuery+=", a."+newAttributeDBName+" as "+newAttribute.toLowerCase();
          }
          else {
            log.debug("SID-243 Adding "+newAttribute+" from the XML");
            if("MySQL".equals(databaseType)) {
              if(fieldIsBoolean) {
                log.debug("SID-244 field "+newAttribute+" was identified as a boolean");
                outerQuery+= ", EXTRACTVALUE(a.attributes, '/Attributes/Map/entry[@key=\""+newAttribute+"\"]/value/Boolean/text()') as "+newAttribute.toLowerCase();
              }
              else {
                log.debug("SID-245 field "+newAttribute+" NOT identified as a boolean");
                outerQuery+= ", EXTRACTVALUE(a.attributes, '/Attributes/Map/entry[@key=\""+newAttribute+"\"]/@value') as "+newAttribute.toLowerCase();
              }
            }
            else if("Microsoft SQL Server".equals(databaseType)) {
              if(fieldIsBoolean) {
                log.debug("SID-246 field "+newAttribute+" was identified as a boolean");
                outerQuery+= ", cast(a.attributes as xml).value('(/Attributes/Map/entry[@key=\""+newAttribute+"\"]/value/Boolean/text())[1]','nvarchar(max)') as "+newAttribute.toLowerCase();
              } 
              else {
                log.debug("SID-247 field "+newAttribute+" NOT identified as a boolean");
                outerQuery+= ", cast(a.attributes as xml).value('(/Attributes/Map/entry[@key=\""+newAttribute+"\"]/@value)[1]','nvarchar(max)') as "+newAttribute.toLowerCase();
              }
            }
            else if("Oracle".equals(databaseType)) {
              if(fieldIsBoolean) {
                log.debug("SID-248 field "+newAttribute+" was identified as a boolean");
                outerQuery+= ", EXTRACTVALUE(XMLTYPE(a.attributes), '/Attributes/Map/entry[@key=\""+newAttribute+"\"]/value/Boolean/text()') as "+newAttribute.toLowerCase();
              }
              else {
                log.debug("SID-249 field "+newAttribute+" NOT identified as a boolean");
                outerQuery+= ", EXTRACTVALUE(XMLTYPE(a.attributes), '/Attributes/Map/entry[@key=\""+newAttribute+"\"]/@value') as "+newAttribute.toLowerCase();
              }
            }
          }
        }
        outerQuery+=" from spt_identity a";
        log.debug("SID-111 outerQuery="+outerQuery);
        boolean whereAdded=false;
        if(onlyActiveIdentities.booleanValue()) {
          outerQuery+=" where a.inactive = 0";
          whereAdded=true;
          log.debug("SID-112 added filter for inactive=false");
        }
        if(onlyCorrelatedIdentities.booleanValue()) {
          if(!whereAdded) {
            outerQuery+=" where a.correlated = 1";
            whereAdded=true;
          }
          else {
            outerQuery+=" and a.correlated = 1";
          }
          log.debug("SID-113 added filter for correlated=true");
        }
        // KCS 2023-02-09 Adding in the comparison logic
        // for the comparison logic we cannot generate histograms
        if(Util.isNotNullOrEmpty(compareField)) {
          if(ignoreNullsOnCompare) {
            if(ignoreCase) {
              // Ignore case and nulls
              // Ignore nulls means that the values must be populated
              if(!whereAdded) {
                outerQuery += " where a."+fieldNameDBName+" IS NOT NULL";
                whereAdded=true;
              }
              else {
                outerQuery += " and a."+fieldNameDBName+" IS NOT NULL";
              }
              outerQuery += " and a."+compareFieldDBName+" IS NOT NULL";
              outerQuery += " and "+stringLengthFunction+"(a."+fieldNameDBName+") > 0";
              outerQuery += " and "+stringLengthFunction+"(a."+compareFieldDBName+") > 0";
              if(Util.isNotNullOrEmpty(searchValue)) {
                outerQuery+=" and upper(a."+fieldNameDBName+") = upper(?)";
              }
              outerQuery += " and upper(a."+fieldNameDBName+") <> upper(a."+compareFieldDBName+")";
            }
            else {
              if(!whereAdded) {
                outerQuery += " where a."+fieldNameDBName+" IS NOT NULL";
                whereAdded=true;
              }
              else {
                outerQuery += " and a."+fieldNameDBName+" IS NOT NULL";
              }
              outerQuery += " and a."+compareFieldDBName+" IS NOT NULL";
              outerQuery += " and "+stringLengthFunction+"(a."+fieldNameDBName+") > 0";
              outerQuery += " and "+stringLengthFunction+"(a."+compareFieldDBName+") > 0";
              if(Util.isNotNullOrEmpty(searchValue)) {
                outerQuery+=" and a."+fieldNameDBName+" = ?";
              }
              outerQuery += " and a."+fieldNameDBName+" <> a."+compareFieldDBName;
            }
          }
          else {
            if(ignoreCase) {
              if(!whereAdded) {
                outerQuery += " where upper(a."+fieldNameDBName+") <> upper(a."+compareFieldDBName+")";
                whereAdded=true;
              }
              else {
                outerQuery += " and upper(a."+fieldNameDBName+") <> upper(a."+compareFieldDBName+")";
              }
            }
            else {
              if(!whereAdded) {
                outerQuery += " where a."+fieldNameDBName+" <> a."+compareFieldDBName;
                whereAdded=true;
              }
              else {
                outerQuery += " and a."+fieldNameDBName+" <> a."+compareFieldDBName;
              }
            }
          }
        }
        else {
          if(Util.isNullOrEmpty(searchValue) || genHistogram) {
            log.debug("SID-114 searchValue is null or generate histogram selected, pulling all application values");
          }
          else if("ISNULL".equalsIgnoreCase(searchValue)) {
            log.debug("SID-114 searchValue specified as ISNULL or isnull");
            if(!whereAdded) {
              outerQuery+=" where ( ( a."+fieldNameDBName+" IS NULL ) or ( "+stringLengthFunction+"(a."+fieldNameDBName+") = 0 ) )";
              whereAdded=true;
            }
            else {
              outerQuery+=" and ( ( a."+fieldNameDBName+" IS NULL ) or ( "+stringLengthFunction+"(a."+fieldNameDBName+") = 0 ) )";
            }
          }
          else if("ISNOTNULL".equalsIgnoreCase(searchValue)) {
            log.debug("SID-114 searchValue specified as ISNOTNULL or isnotnull");
            if(!whereAdded) {
              outerQuery+=" where ( ( a."+fieldNameDBName+" IS NOT NULL ) and ( "+stringLengthFunction+"(a."+fieldNameDBName+") > 0 ) )";
              whereAdded=true;
            }
            else {
              outerQuery+=" and ( ( a."+fieldNameDBName+" IS NOT NULL ) and ( "+stringLengthFunction+"(a."+fieldNameDBName+") > 0 ) )";
            }
          }
          else {
            if(ignoreCase) {
              if(!whereAdded) {
                outerQuery+=" where upper(a."+fieldNameDBName+") = upper(?)";
                whereAdded=true;
              }
              else {
                outerQuery+=" and upper(a."+fieldNameDBName+") = upper(?)";
              }
              log.debug("SID-115 added case insensitive field name search");
            }
            else {
              if(!whereAdded) {
                outerQuery+=" where a."+fieldNameDBName+" = ?";
                whereAdded=true;
              }
              else {
                outerQuery+=" and a."+fieldNameDBName+" = ?";
              }
              log.debug("SID-116 added case sensitive field name search");
            }
          }
        }
        outerQuery+=" order by a.name";
        log.debug("SID-117 final outerQuery="+outerQuery);
        pstmt=sqlConnection.prepareStatement(outerQuery);
        if(Util.isNullOrEmpty(searchValue) || genHistogram) {
          log.debug("SID-114 searchValue is null or generate histogram selected, pulling all application values");
        }
        else if("ISNULL".equalsIgnoreCase(searchValue)) {
          log.debug("SID-114 searchValue specified as ISNULL or isnull");
        }
        else if("ISNOTNULL".equalsIgnoreCase(searchValue)) {
          log.debug("SID-114 searchValue specified as ISNOTNULL or isnotnull");
        }
        else {
          pstmt.setString(1,searchValue);
        }
        updateProgress(context, result, "SQL Query built, executing");
        queryResult=pstmt.executeQuery();
        if(queryResult!=null) {
          goodResults=true;
          updateProgress(context, result, "SQL Query successfully executed");
        }
      }
      catch(Exception qex) {
        log.error("SID-120 "+qex.getClass().getName()+":"+qex.getMessage());
        updateProgress(context, result, "SQL Query execution failed: "+qex.getClass().getName());
      }
    }
    else {
      /*
       * If onlyActiveIdentities, add that filter
       */
      if(onlyActiveIdentities.booleanValue()) {
        Filter activeFilter=Filter.eq("inactive", false);
        filterList.add(activeFilter);
        log.debug("SID-046 added filter for inactive=false");
      }
      /*
       * If onlyCorrelatedIdentities, add that filter
       */
      if(onlyCorrelatedIdentities.booleanValue()) {
        Filter correlatedFilter=Filter.eq("correlated", true);
        filterList.add(correlatedFilter);
        log.debug("SID-047 added filter for correlated=true");
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
      //if(filterList.size()==1) {
      //  qo.addFilter(appFilter);
      //}
      //else {
        qo.addFilter(Filter.and(filterList));
      //}
      log.debug("SID-032 Creating IncrementalObjectIterator");
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
    log.debug("SID-033 Have iterator, creating maps");
    updateProgress(context, result, "Creating analysis maps");
    
    List<Map<String,Map<String,String>>> searchArgs=new ArrayList<Map<String,Map<String,String>>>();
    List<String> searchOps=new ArrayList<String>();
    String suffix="";
    
    if(Util.isNotNullOrEmpty(multiSearch) && (!multiSearchIsFilter)) {
      parseSearchLogic(multiSearch, searchArgs, searchOps);
      log.debug("SID-011 searchArgs="+searchArgs.toString());
      log.debug("SID-012 searchOps="+searchOps.toString());
      genHistogram=false;
    }
    
    if(rawcount>0) {
      /*
       * rawcount is only > 0 if non-SQL (API) is used
       * and identityListRaw will also be populated
       */
      for(String identId: identityListRaw) {
        Identity identObj = context.getObjectById(Identity.class,identId);
        itercount++;
        String nativeIdentity=identObj.getName();
        log.debug("SID-035 Found account "+nativeIdentity);
        matchedValue=false;
        addToList=false;
        if(Util.isNullOrEmpty(multiSearch) || multiSearchIsFilter) {
          /*
           * Search on a field for a value or another field
           *
           * If the field is not a String then prepend the class name to its toString value
           * Pull the fieldValue from fieldName of the link
           */
          Object fieldValueObj=identObj.getAttribute(fieldName);
          /*
           * To support attribute changes functionality
           */
          newAttributeValueObj=null;
          newAttributeValueStr=null;
          if(newAttribute!=null) {
            newAttributeValueObj=identObj.getAttribute(newAttribute);
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
          log.debug("SID-036 fieldValue="+fieldValue);
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
                log.debug("SID-037 matched "+searchValue);
              }
            }
            else {
              if(fieldValue.equals(searchValue)) {
                count++;
                checkCorr=true;
                matchedValue=true;
                log.debug("SID-038 matched "+searchValue);
              }
            }
            addToList=matchedValue;
          }
          else {
            Object compareValueObj=null;
            compareValueObj = identObj.getAttribute(compareField);
            log.debug("SID-039 compareValue="+compareValueObj);
            localSearchValue="NULL";
            if(compareValueObj==null) {
              log.debug("SID-040 compareValue is null");
            }
            else {
              if(compareValueObj instanceof String) {
                localSearchValue=(String)compareValueObj;
              }
              else {
                localSearchValue=compareValueObj.getClass().getName()+":"+compareValueObj.toString();
              }
            }
            log.debug("SID-041 localSearchValue="+localSearchValue);
            if(ignoreCase) {
              if(fieldValue.equalsIgnoreCase(localSearchValue)) {
                count++;
                checkCorr=true;
                matchedValue=true;
                log.debug("SID-042 matched "+localSearchValue);
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
                log.debug("SID-043 matched "+localSearchValue);
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
                log.debug("SID-044 this account is not correlated to an Identity");
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
            log.debug("SID-091 setThisValue="+setThisValue+" setAllNewValues="+setAllNewValues+" deleteNewValue="+deleteNewValue);
            if(setThisValue) {
              if(newAttributeValueObj==null && deleteNewValue) {
                log.debug("SID-086 the value is already null, so there is no need to change it");
              }
              else if(newAttributeValueStr!=null && newAttributeValueStr.equals(newValue)) {
                log.debug("SID-087 the value is already the new value, so there is no need to change it");
              }
              else {
                Identity lockedIdentity = null;
                if(identObj!=null) {
                  log.debug("SID-063 on identity "+identObj.getName()+" going to set "+newAttribute+" to "+newValue);
                  try {
                    String identObjName=identObj.getName();
                    context.decache(identObj);
                    lockedIdentity = acquireIdentityLock(context, identObjName, "Locking to remove value", 60, 0);
                    if(lockedIdentity!=null) {
                      log.debug("SID-067 locked the identity "+identObjName);
                      if(deleteNewValue) {
                        lockedIdentity.setAttribute(newAttribute,null);
                        log.debug("SID-068 cleared "+newAttribute);
                      }
                      else {
                        lockedIdentity.setAttribute(newAttribute,newValue);
                        log.debug("SID-069 set "+newAttribute+" to "+newValue);
                      }
                      context.saveObject(lockedIdentity);
                      context.commitTransaction();
                      log.debug("SID-085 saved the user "+identObjName);
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
          log.debug("SID-021 Num searches is "+numSearches);
          updateProgress(context, result, "Multi search selected");
          for(Map<String,Map<String,String>> expMap: searchArgs) {
            String op=searchOps.get(searchNum);
            log.debug("SID-022 Search op = "+op);
            passThis=false;
            for(String fieldName: expMap.keySet()) {
              Object fieldValueObj=identObj.getAttribute(fieldName);
              fieldValue="null";
              if(fieldValueObj != null) {
                nncount++;
                if(fieldValueObj instanceof String) {
                  fieldValue=(String)fieldValueObj;
                }
                else {
                  fieldValue=fieldValueObj.getClass().getName()+":"+fieldValueObj.toString();
                }
                log.debug("SID-023 Comparing fieldName = "+fieldName+" and value "+fieldValue);
                Map<String,String> compMap=expMap.get(fieldName);
                for(String val: compMap.keySet()) {
                  String opStr=compMap.get(val);
                  log.debug("SID-024 Against value of "+val+" with operation "+opStr);
                  if("EQ".equals(opStr)) {
                    if(val.equals(fieldValue)) {
                      log.debug("SID-025 val==fieldValue for "+fieldName+" passing this");
                      passThis=true;
                      checkCorr=true;
                    }
                  }
                  if("NE".equals(opStr)) {
                    if(!(val.equals(fieldValue))) {
                      log.debug("SID-026 val!=fieldValue for "+fieldName+" passing this");
                      passThis=true;
                      checkCorr=true;
                    }
                  }
                }
              }
            }
            log.debug("SID-045 passThis = "+passThis+" for fieldName="+fieldName);
            if("OR".equals(op)) {
              if(passThis)passAll=true;
              log.debug("SID-046 op=OR so passAll = true");
            }
            else if("AND".equals(op)) {
              checkAll=true;
              log.debug("SID-047 op=AND so checkAll = true");
            }
            if(checkAll) {
              if(searchNum==0) {
                passAll=passThis;
                log.debug("SID-048 searchNum=0 and checkAll=true, passAll="+passAll);
              }
              else {
                if(!passThis) {
                  log.debug("SID-049 passThis=false so passAll=false");
                  passAll=false;
                }
              }
            }
            searchNum++;
          }
          if(passAll) {
            log.debug("SID-050 passAll=true so increment count");
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
          log.debug("SID-092 Attempting to update progress with value:"+String.format("%6d",itercount)+" "+nativeIdentity);
          updateProgress(context, result, String.format("%6d",itercount)+" "+nativeIdentity);
          log.debug("SID-093 updateProgress finished");
        }
        //sboutput.append("\nLink for "+nativeIdentity+" has "+fieldName+"="+fieldValue);
        if(checkCorr) {
          log.debug("SID-094 Check correlation selected");
          if(identObj==null) {
            sboutput.append("\nLink for "+nativeIdentity+" has null identity");
          }
          else {
            if(identObj.isCorrelated()) {
              corrcount++;
              log.debug("SID-095 identity "+identObj.getName()+" is correlated");
            }
            else {
              noncorrcount++;
              log.debug("SID-096 identity "+identObj.getName()+" is not correlated");
            }
            // Now check the type
            String identityType=identObj.getType();
            log.debug("SID-097 identity "+identObj.getName()+" type is "+identityType);
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
        //context.decache(link);
        context.decache(identObj);
      }
    }
    else if(goodResults) {
      /*
       * goodResults only populated if results were obtained from the database
       */
      log.debug("SID-121 a good result was obtained, iterating the queryResult");
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
        //String acctNativeIdentity=queryResult.getString("native_identity");
        //Boolean acctDisabled=false;
        //if(CommonMethods.isVersionGE(sailpointVersion,sailpointPatch,"8.2","")) {
        //  acctDisabled=queryResult.getBoolean("iiq_disabled");
        //}
        //else {
        //  String acctDisabledStr=queryResult.getString("iiqdisabled");
        //  if("true".equals(acctDisabledStr)) {
        //    acctDisabled=true;
        //  }
        //  else {
        //    acctDisabled=false;
        //  }
        //}
        String acctFieldValue=queryResult.getString(fieldName.toLowerCase());
        log.debug("SID-122 found identity "+identname+" : "+identds);
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
          log.debug("SID-140 searchValue is null or empty, count="+count+" nncount="+nncount+" value="+acctFieldValue);
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
            log.debug("SID-142 searchValue is null or empty, count="+count+" nncount="+nncount+" value="+acctFieldValue);
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
            log.debug("SID-143 searchValue is not null or empty, count="+count+" nncount="+nncount+" value="+acctFieldValue);
          }
        }
        else {
          log.debug("SID-141 searchValue is not null or empty, value="+acctFieldValue);
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
          log.debug("SID-191 setThisValue="+setThisValue+" setAllNewValues="+setAllNewValues+" deleteNewValue="+deleteNewValue);
          if(setThisValue) {
            
            if(newAttributeValueObj==null && deleteNewValue) {
              log.debug("SID-186 the value is already null, so there is no need to change it");
            }
            else if(newAttributeValueStr!=null && newAttributeValueStr.equals(newValue)) {
              log.debug("SID-187 the value is already the new value, so there is no need to change it");
            }
            else {
              Identity lockedIdentity = null;
              log.debug("SID-170 on identity "+identname+" going to set "+newAttribute+" to "+newValue);
              try {
                lockedIdentity = acquireIdentityLock(context, identname, "Locking to remove value", 60, 0);
                if(lockedIdentity!=null) {
                  log.debug("SID-171 locked the identity "+identname);
                  //Link link=null;
                  //List<Link> links = idService.getLinks(lockedIdentity,appObj);
                  //for(Link alink: links) {
                  //  if(link==null) {
                  //    link=alink;
                  //    log.debug("SID-035 found first link "+alink.getNativeIdentity());
                  //  }
                  //  else {
                  //    log.debug("SID-035 found multiple links including "+alink.getNativeIdentity());
                  //  }
                  //}
                  if(deleteNewValue) {
                    lockedIdentity.setAttribute(newAttribute,null);
                    log.debug("SID-172 cleared "+newAttribute);
                  }
                  else {
                    lockedIdentity.setAttribute(newAttribute,newValue);
                    log.debug("SID-173 set "+newAttribute+" to "+newValue);
                  }
                  context.saveObject(lockedIdentity);
                  context.commitTransaction();
                  log.debug("SID-188 saved the user "+identname);
                }
              }
              catch (Exception ex) {
                log.error("SID-189 Trying to modify a value:"+ex.getClass().getName()+":"+ex.getMessage());
              }
              finally {
                if(lockedIdentity!=null) {
                  try {
                    ObjectUtil.unlockIdentity(context,lockedIdentity);
                    context.decache(lockedIdentity);
                  }
                  catch (Exception fex) {
                    log.error("SID-190 Trying to unlock an identity:"+fex.getClass().getName()+":"+fex.getMessage());
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
    log.debug("SID-125 Out of "+itercount+" identities, not null= "+nncount);
    sboutput.append("\nOut of "+itercount+" identities, not null= "+nncount);
    if(Util.isNotNullOrEmpty(fieldName) && Util.isNotNullOrEmpty(searchValue)) {
      log.debug("SID-126 Count of "+fieldName+"="+searchValue+", count = "+count);
      sboutput.append("\nCount of "+fieldName+"="+searchValue+", count = "+count);
    }
    else if(Util.isNotNullOrEmpty(multiSearch) && (!multiSearchIsFilter)) {
      log.debug("SID-127 Count of multi valued search = "+count);
      sboutput.append("\nCount of multi valued search = "+count);
    }
    log.debug("SID-128 Of these, "+corrcount+" are correlated identities");
    sboutput.append("\nOf these, "+corrcount+" are correlated identities");
    log.debug("SID-129 and "+noncorrcount+" are uncorrelated identities");
    sboutput.append("\nand "+noncorrcount+" are uncorrelated identities");
    log.debug("SID-130 Identity Type counts:");
    sboutput.append("\nIdentity Type counts:");
    for(String typeStr:typeMap.keySet()) {
      log.debug("SID-131 "+String.format("%20s",typeStr)+" count = "+typeMap.get(typeStr));
      sboutput.append("\n"+String.format("%20s",typeStr)+" count = "+typeMap.get(typeStr));
    }
    if(genHistogram) {
      updateProgress(context, result, "Generating histogram sort: "+histoSort);
      log.debug("SID-132 Histogram of "+fieldName+" values");
      sboutput.append("\n\nHistogram of "+fieldName+" values");
      log.debug("SID-162 creating the sorted maps");
      SortedSet<String> valueSet=new TreeSet<String>();
      SortedSet<Integer> revcountSet=new TreeSet<Integer>(Collections.reverseOrder());
      SortedSet<String> countvalueSet=new TreeSet<String>();
      int maxKeyLen=10;
      for(String histKey: histogramMap.keySet()) {
        Integer histValue=histogramMap.get(histKey);
        log.debug("SID-163 Adding "+histKey+" to valueSet");
        valueSet.add(histKey);
        log.debug("SID-164 Adding "+histValue+" to revcountSet");
        revcountSet.add(histValue);
        String countvalue=String.format("%d",histValue.intValue());
        countvalue="000000000"+countvalue;
        countvalue=countvalue.substring(countvalue.length()-9);
        log.debug("SID-165 Adding "+countvalue+"-"+histKey+" to countvalueSet");
        countvalueSet.add(countvalue+"-"+histKey);
        int keyLen=histKey.length();
        if(keyLen>maxKeyLen)maxKeyLen=keyLen;
      }
      String keyFormat="%"+String.format("%d",maxKeyLen)+"s";
      log.debug("SID-166 maxKeyLen="+maxKeyLen+" keyFormat="+keyFormat);
      if("value".equalsIgnoreCase(histoSort)) {
        log.debug("SID-167 histoSort=value, sorting on value using valueSet");
        for(String histKey: valueSet) {
          Integer histValue=histogramMap.get(histKey);
          log.debug("SID-133 "+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
          sboutput.append("\n"+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
        }
      }
      else if("asc".equalsIgnoreCase(histoSort)) {
        log.debug("SID-168 histoSort=asc, sorting on count using countvalueSet");
        for(String countvalueKey: countvalueSet) {
          Integer histValue=Integer.valueOf(countvalueKey.substring(0,9));
          String histKey=countvalueKey.substring(10);
          log.debug("SID-133 "+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
          sboutput.append("\n"+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
        }
      }
      else if("desc".equalsIgnoreCase(histoSort)) {
        log.debug("SID-169 histoSort=desc, sorting descending count using revcountSet");
        for(Integer revcountKey: revcountSet) {
          String countvalue=String.format("%d",revcountKey.intValue());
          countvalue="000000000"+countvalue;
          countvalue=countvalue.substring(countvalue.length()-9);
          for(String countvalueKey: countvalueSet) {
            if(countvalueKey.startsWith(countvalue)) {
              Integer histValue=Integer.valueOf(countvalueKey.substring(0,9));
              String histKey=countvalueKey.substring(10);
              log.debug("SID-133 "+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
              sboutput.append("\n"+String.format(keyFormat,histKey)+":"+String.format("%5d",histValue.intValue()));
            }
          }
        }
      }
      else {
        for(String histKey: histogramMap.keySet()) {
          Integer histValue=histogramMap.get(histKey);
          log.debug("SID-133 "+String.format("%20s",histKey)+":"+String.format("%5d",histValue.intValue()));
          sboutput.append("\n"+String.format("%20s",histKey)+":"+String.format("%5d",histValue.intValue()));
        }
      }
    }
    if(genIdentityList) {
      updateProgress(context, result, "Generating identity list");
      if(Util.isNullOrEmpty(compareField)) {
        log.debug("SID-134 List of matched identities");
        sboutput.append("\n\nList of matched identities");
      }
      else {
        log.debug("SID-135 List of identities not matching");
        sboutput.append("\n\nList of identities not matching");
      }
      int listCount=0;
      int listLength=identityList.size();
      // Analyze listModuloStr
      listModuloStr=listModuloStr.trim();
      listModulo=1;
      // If it ends with a % sign
      if (listModuloStr.endsWith("%")) {
        log.debug("SID-138 found listModuloStr ends with %");
        // User wants a percent of the users
        double hundred=100.0;
        double listPct=100.0;
        int llmslen=listModuloStr.length();
        // This should be the string minus the %
        String listModuloStrNP=listModuloStr.substring(0,llmslen-1);
        log.debug("SID-139 listModuloStrNP="+listModuloStrNP);
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
        log.debug("SID-138 identityList is empty, proceeding");
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
            log.debug("SID-136 "+identValue);
            sboutput.append("\n"+identValue);
          }
        }
      }
    }
    if(genIdentityFilter) {
      log.debug("SID-110 filter of names");
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
    log.debug("SID-137 processed");
    sboutput.append("\n\nprocessed");
    updateProgress(context, result, "Wrapping up");
    /*
     * Display the results
     */
    log.debug("SID-111 totalCount="+totalCount);
    result.setAttribute("totalCount", count);
    result.setAttribute("resultString", sboutput.toString());
    taskResult.setCompletionStatus(TaskResult.CompletionStatus.Success);
    taskResult.addMessage(new Message(Message.Type.Info,"Processed"));
    log.debug("SID-112 exiting");
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
        log.debug("SID-051 Found "+opStr+" at "+flogIndex);
      }
      if(landIndex>0 &&  flogIndex<0) {
        flogIndex=landIndex;
        flogLen=2;
        opStr="AND";
        log.debug("SID-052 Found "+opStr+" at "+flogIndex);
      }
      if(corIndex>0 &&  flogIndex<0) {
        flogIndex=corIndex;
        flogLen=2;
        opStr="OR";
        log.debug("SID-053 Found "+opStr+" at "+flogIndex);
      }
      if(lorIndex>0 &&  flogIndex<0) {
        flogIndex=lorIndex;
        flogLen=2;
        opStr="OR";
        log.debug("SID-054 Found "+opStr+" at "+flogIndex);
      }
      if(candIndex>0 &&  candIndex<flogIndex) {
        flogIndex=candIndex;
        flogLen=3;
        opStr="AND";
        log.debug("SID-055 Found "+opStr+" at "+flogIndex);
      }
      if(landIndex>0 &&  landIndex<flogIndex) {
        flogIndex=landIndex;
        flogLen=2;
        opStr="AND";
        log.debug("SID-056 Found "+opStr+" at "+flogIndex);
      }
      if(corIndex>0 &&  corIndex<flogIndex) {
        flogIndex=corIndex;
        flogLen=2;
        opStr="OR";
        log.debug("SID-057 Found "+opStr+" at "+flogIndex);
      }
      if(lorIndex>0 &&  lorIndex<flogIndex) {
        flogIndex=lorIndex;
        flogLen=2;
        opStr="OR";
        log.debug("SID-058 Found "+opStr+" at "+flogIndex);
      }
      if(flogIndex<0) {
        // there is no logic symbol
        String logicalExpression=workingInput;
        log.debug("SID-061 Found logical expression "+logicalExpression);
        foundError=addLogicalExpression(logicalExpression,searchArgs,"None",searchOps);
        workingInput="";
      }
      else {
        String logicalExpression=workingInput.substring(0,flogIndex);
        log.debug("SID-063 Found logical expression "+logicalExpression);
        foundError=addLogicalExpression(logicalExpression,searchArgs,opStr,searchOps);
        workingInput=workingInput.substring(flogIndex+flogLen);
        log.debug("SID-064 workingInput is now "+workingInput);
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
      log.debug("SID-071 found opStr "+opStr+" at "+opIndex);
    }
    if(neIndex>0 && opIndex<0) {
      opIndex=neIndex;
      opStr="NE";
      opLen=2;
      log.debug("SID-072 found opStr "+opStr+" at "+opIndex);
    }
    if(ltIndex>0 && leIndex<0 && opIndex<0) {
      opIndex=ltIndex;
      opStr="LT";
      opLen=1;
      log.debug("SID-073 found opStr "+opStr+" at "+opIndex);
    }
    if(leIndex>0 && opIndex<0) {
      opIndex=leIndex;
      opStr="LE";
      opLen=2;
      log.debug("SID-074 found opStr "+opStr+" at "+opIndex);
    }
    if(gtIndex>0 && geIndex<0 && opIndex<0) {
      opIndex=gtIndex;
      opStr="GT";
      opLen=1;
      log.debug("SID-075 found opStr "+opStr+" at "+opIndex);
    }
    if(geIndex>0 && opIndex<0) {
      opIndex=geIndex;
      opStr="GE";
      opLen=2;
      log.debug("SID-076 found opStr "+opStr+" at "+opIndex);
    }
    if(nnIndex>0 && opIndex<0) {
      opIndex=nnIndex;
      opStr="NN";
      opLen=7;
      log.debug("SID-077 found opStr "+opStr+" at "+opIndex);
    }
    if(nuIndex>0 && opIndex<0) {
      opIndex=nuIndex;
      opStr="NU";
      opLen=6;
      log.debug("SID-078 found opStr "+opStr+" at "+opIndex);
    }
    if(opIndex<0) {
      log.error("SID-079 Did not find a logic operation in this expression: "+exp);
      return true;
    }
    else {
      log.debug("SID-080 Found logical expression "+opStr+" at character "+opIndex+" in "+exp);
    }
    String arg=exp.substring(0,opIndex).trim();
    log.debug("SID-081 Found argument "+arg);
    String val="NULL";
    if(opLen<=2) {
      val=exp.substring(opIndex+opLen).trim();
    }
    log.debug("SID-082 Found expression "+arg+" "+opStr+" "+val);
    Map<String,String> compMap=new HashMap<String,String>();
    compMap.put(val,opStr);
    log.debug("SID-083 Created compMap="+compMap.toString());
    Map<String,Map<String,String>>expMap=new HashMap<String,Map<String,String>>();
    expMap.put(arg,compMap);
    log.debug("SID-084 Created expMap="+expMap.toString());
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
  private String generateDBFieldFromIdField(String field) {
    String dbfield="";
    char[] fieldChars=field.toCharArray();
    for (int ic=0; ic<fieldChars.length; ic++) {
      if(Character.isUpperCase(fieldChars[ic])) {
        dbfield=dbfield+"_";
      }
      dbfield=dbfield+fieldChars[ic];
    }
    return dbfield.toLowerCase();
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
