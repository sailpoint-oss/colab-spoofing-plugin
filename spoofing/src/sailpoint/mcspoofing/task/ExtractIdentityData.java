package sailpoint.mcspoofing.task;

//import org.apache.log4j.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import sailpoint.api.SailPointContext;
import sailpoint.api.IdentityService;
import sailpoint.api.IncrementalObjectIterator;
import sailpoint.object.Attributes;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskSchedule;
import sailpoint.object.Application;
import sailpoint.object.Identity;
import sailpoint.object.Link;
import sailpoint.object.QueryOptions;
import sailpoint.object.Schema;
import sailpoint.object.Filter;
import sailpoint.object.ObjectConfig;
import sailpoint.object.ObjectAttribute;
import sailpoint.object.SearchInputDefinition;
import sailpoint.task.AbstractTaskExecutor;
import sailpoint.task.BasePluginTaskExecutor;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;

/**
 * Extract data from any account for future aggregation.  This assumes all values are single strings
 * Have not accounted for multivalued strings.  Normally used for authoritative sources
 * @author keith
 */
public class ExtractIdentityData extends BasePluginTaskExecutor {
  private static Log log = LogFactory.getLog(ExtractIdentityData.class);
  
  private String applicationName="";
  private List<String> applicationNames = new ArrayList<String>();
  private Application application=null;
  private List<String> processIdentities = new ArrayList<String>();
  private String folderName="";
  private String defaultFilename="$applicationName$-$identityName$";
  private String filterString=null;
  private String orderbyString=null;
  private String excludeListStr=null;
  private String includeListStr=null;
  private List<String> excludeList=new ArrayList<String>();
  private List<String> includeList=new ArrayList<String>();
  private Boolean getAllAccounts=false;
  private Boolean onlyActiveIdentities=true;
  private Boolean onlyCorrelatedIdentities=true;
  private Boolean printSchemaParams=false;
  private List<String> schemaParamsList=new ArrayList<String>();
  private String schemaParamsStr="";
  private StringBuffer sboutput=new StringBuffer();
  private long oldestDate=0L;
  private TaskResult taskResult=null;
  
  @SuppressWarnings({"rawtypes","unchecked"})
  @Override
  public void execute(SailPointContext context, TaskSchedule schedule, TaskResult result, Attributes<String, Object> args)
    throws Exception {
    
    log.debug("EID-001 Starting Extract Account Data task");
    DateFormat sdfout=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    DateFormat sdfnotime=new SimpleDateFormat("MM/dd/yyyy");
    Date now=new Date();
    Date nowNoTime=now;
    String nowDateNoTimeStr=sdfnotime.format(now);
    try {
      nowNoTime=sdfnotime.parse(nowDateNoTimeStr);
    }
    catch (Exception et) {}
    oldestDate=nowNoTime.getTime();
    sboutput.append("ExtractIdentityData started at "+sdfout.format(now)+"\n");
    /*
     * Determine if this is a Windows or Linux system
     */
    String folderSep=File.separator;
    if(folderSep.equals("/")) {
      folderName="~"+folderSep+"extractdata";
    }
    else {
      folderName="C:"+folderSep+"ExtractData";
    }
    log.debug("EID-002 initial folderName="+folderName);
    taskResult = result;
    /*
     * Get the arguments from the configuration
     * Arguments from the TaskDefinition Plugin Identity Extract:
     * identities - this is a list of identity names that were individually selected
     * selectall  - this implies we would like to select all identities,
     *              subject to conditions below.
     * onlyActiveIdentities - select all active identities
     * onlyTheseTypes       - exclude identities not in one of these types
     * applications         - only include identities with one of these links
     * filterString         - forget the above 3 and use this filter, which
     *                        was built with the Advanced Search page.
     */
    if(args==null || args.isEmpty()) {
      log.warn("EID-003 args is null or empty, exiting, need at least one application");
      result.setCompleted(new Date());
      result.setCompletionStatus(TaskResult.CompletionStatus.Error);
      result.setAttribute("resultString", sboutput.toString());
      context.saveObject(result);
      context.commitTransaction();
      return;
    }
    /*
     * Gather the applications
     */
    List<Object> applications = new ArrayList<Object>();
    if(args.containsKey("applications")) {
      log.debug("EID-040 found applications in args");
      applications=args.getList("applications");
      log.debug("EID-004 List of applications="+applications.toString());
    }
    else {
      log.debug("EID-041 did not find applications in args");
    }
    /*
     * Change made KCS 07/22/2023 to change what gets printed
     * literally use the input string, requires the first element to be
     * the nativeIdentity
     */
    printSchemaParams=args.getBoolean("printSchema");
    schemaParamsStr=args.getString("schemaParams");
    if(Util.isNotNullOrEmpty(schemaParamsStr)) {
      String[] schemaParamsArray=schemaParamsStr.split(",");
      schemaParamsList=new ArrayList<String>(Arrays.asList(schemaParamsArray));
    }
    /*
     * Gather the identities
     */
    List identities=new ArrayList();
    if(args.containsKey("identities")) {
      identities = args.getList("identities");
      log.debug("EID-005 Identities List:" + identities.toString());
    }
    String identityName = "";
    Identity identity=null;
    String identityNameTag="$identityName$";
    String dateTag="$date$";
    result.put("internalStartDate", now);
    /*
     * Parse the applications list into a list of application names
     */
    for(Object app: applications) {
      log.debug("EID-006 Application class:"+app.getClass().getName());
      if(app instanceof String) {
        applicationName = (String)app;
        if(applicationName.contains(",")) {
          for (String workingApp : applicationName.split(",") ) {
            log.debug("EID-016 Application added to list:"+workingApp);
            applicationNames.add(workingApp);
          }
        }
        else {
          log.debug("EID-007 Application added to list:"+applicationName);
          applicationNames.add(applicationName);
        }
      }
    }
    /*
     * Parse the identities list into a list of identity ids
     */
    if(identities!=null && !identities.isEmpty()) {
      for(Object ident: identities) {
        log.debug("EID-008 Identity class:"+ident.getClass().getName());
        if(ident instanceof String) {
          if ( ((String)ident).contains(",") ) {
            for( String workingIdentity : ((String)ident).split( "," ) ) {
              identityName = ((String)workingIdentity).trim();
              log.debug("EID-009 Identity selected:"+identityName);
              identity = context.getObjectByName(Identity.class, identityName);
              if (identity != null ) {
                log.debug("EID-010 identityName="+identity.getName());
              }
              else {
                log.debug("EID-011 Null Identity Returned for: " + identityName);
              }
              processIdentities.add(identity.getId());
              context.decache(identity);
            }
          }
          else {
            identityName = (String)ident;
            log.debug("EID-012 Identity selected:"+identityName);
            identity = context.getObjectByName(Identity.class, identityName);
            if (identity != null ) {
              log.debug("EID-013 identityName="+identity.getName());
            }
            else {
              log.debug("EID-014 Null Identity Returned for: " + identityName);
            }
            processIdentities.add(identity.getId());
            context.decache(identity);
          }
        }
      }
    }
    log.debug("EID-015 processIdentities after update="+processIdentities.toString());
    if(processIdentities.size()>0) {
      if(processIdentities.size()==1) {
        updateProgress(context, result, "Identity selected: "+identityName);
      }
      else {
        updateProgress(context, result, "Multiple Identities selected");
      }
    }
    IdentityService idService=new IdentityService(context); 
    /*
     * getAllAccounts will override the identity selection and select all accounts
     */
    getAllAccounts=args.getBoolean("selectall");
    log.debug("EID-017 getAllAccounts="+getAllAccounts.toString());
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
     * onlyTheseTypes is the array of identity types like employee, contractor, etc.
     */
    String onlyTheseTypesStr=args.getString("onlyTheseTypes");
    List<String> onlyTheseTypes=new ArrayList<String>();
    if(onlyTheseTypesStr!=null && !onlyTheseTypesStr.isEmpty()) {
      if(onlyTheseTypesStr.contains(",")) {
        String[] onlyTheseTypesArray=onlyTheseTypesStr.split(",");
        for(int ocount=0; ocount<onlyTheseTypesArray.length; ocount++) {
          onlyTheseTypes.add(onlyTheseTypesArray[ocount].trim());
        }
      }
      else {
        onlyTheseTypes.add(onlyTheseTypesStr.trim());
      }
    }
    log.debug("EID-026 onlyTheseTypes="+onlyTheseTypes.toString());
    /*
     * Lastly a filterString will override everything
     */
    if(args.containsKey("filterString")) {
      filterString = args.getString("filterString");
      log.debug("EID-030 filterString="+filterString);
    }
    else {
      log.debug("EID-031 did not find filterString in args");
    }
    if(args.containsKey("orderby")) {
      orderbyString = args.getString("orderby");
      log.debug("EID-032 orderby="+orderbyString);
    }
    else {
      log.debug("EID-033 did not find orderbyString in args");
    }
    if(args.containsKey("excludeList")) {
      excludeListStr = args.getString("excludeList");
      log.debug("EID-070 excludeList="+excludeListStr);
    }
    else {
      log.debug("EID-071 did not find excludeList in args");
    }
    if(args.containsKey("includeList")) {
      includeListStr = args.getString("includeList");
      log.debug("EID-072 includeList="+includeListStr);
    }
    else {
      log.debug("EID-073 did not find includeList in args");
    }
    /*
     * Now that the inputs are in, figure out what to do
     * Set orderings if specified
     */
    QueryOptions qo=new QueryOptions();
    List<Filter> filterList=new ArrayList<Filter>();
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
    if(getAllAccounts.booleanValue()) {
      log.debug("EID-044 getAllAccounts is true, checking criteria");
      /*
       * If there is a specified filter, compile and add it.
       */
      if(Util.isNotNullOrEmpty(filterString)) {
        log.debug("EID-045 filterString specified, using it:"+filterString);
        Filter compiledFilter=Filter.compile(filterString);
        filterList.add(compiledFilter);
      }
      /*
       * If onlyActiveIdentities, add that filter
       */
      if(onlyActiveIdentities.booleanValue()) {
        Filter activeFilter=Filter.eq("inactive", false);
        filterList.add(activeFilter);
        log.debug("EID-046 added filter for inactive=false");
      }
      /*
       * If onlyCorrelatedIdentities, add that filter
       */
      if(onlyCorrelatedIdentities.booleanValue()) {
        Filter correlatedFilter=Filter.eq("correlated", true);
        filterList.add(correlatedFilter);
        log.debug("EID-047 added filter for correlated=true");
      }
      if(!onlyTheseTypes.isEmpty()) {
        log.debug("EID-048 Found data in onlyTheseTypes:"+onlyTheseTypes.toString());
        if(onlyTheseTypes.size()==1) {
          Filter typeFilter=Filter.eq("type", onlyTheseTypes.get(0));
          filterList.add(typeFilter);
          log.debug("EID-049 Only one type, adding type="+onlyTheseTypes.get(0));
        }
        else {
          log.debug("EID-050 Found more than one type");
          List<Filter> onlyTypeList=new ArrayList<Filter>();
          for(String onlyThisType: onlyTheseTypes) {
            Filter typeFilter=Filter.eq("type", onlyThisType);
            onlyTypeList.add(typeFilter);
            log.debug("EID-051 Adding type="+onlyThisType);
          }
          filterList.add(Filter.or(onlyTypeList));
          log.debug("EID-052 Added or filter with all of the types");
        }
      }
      if(!applicationNames.isEmpty()) {
        log.debug("EID-053 ApplicationNames is populated:"+applicationNames.toString());
        if(applicationNames.size()==1) {
          Filter applicationFilter=Filter.eq("links.application.name",applicationNames.get(0));
          filterList.add(applicationFilter);
          log.debug("EID-054 Added application filter on "+applicationNames.get(0));
        }
        else {
          log.debug("EID-055 Found multiple applications:");
          List<Filter> applicationFilterList=new ArrayList<Filter>();
          for(String appName: applicationNames) {
            Filter appNameFilter=Filter.eq("links.application.name", appName);
            applicationFilterList.add(appNameFilter);
            log.debug("EID-056 Added application filter on "+appName);
          }
          filterList.add(Filter.or(applicationFilterList));
          log.debug("EID-057 Added or filter with all of the applications");
        }
      }
      if(!filterList.isEmpty()) {
        qo.addFilter(Filter.and(filterList));
      }
      IncrementalObjectIterator it=new IncrementalObjectIterator(context,Identity.class,qo);
      while(it.hasNext()) {
        Identity passesCheck = (Identity)it.next();
        log.trace("EID-035 Identity:"+passesCheck.getName()+" passes checks");
        processIdentities.add(passesCheck.getId());
        context.decache(passesCheck);
      }
      Integer sizeInt=Integer.valueOf(processIdentities.size());
      updateProgress(context, result, "All accounts selected, count="+sizeInt.toString());
      log.debug("EID-036 All accounts selected, count="+sizeInt.toString());
    }
    if(processIdentities.size()==0) {
      updateProgress(context, result, "No accounts selected, exiting");
      result.setCompleted(new Date());
      result.setCompletionStatus(TaskResult.CompletionStatus.Error);
      result.setAttribute("resultString", sboutput.toString());
      context.saveObject(result);
      context.commitTransaction();
      return;
    }
    /*
     * Get the folder name
     */
    String folderNameInput=args.getString("folderpath");
    log.debug("EID-018 folderpath:"+folderNameInput);
    if(Util.isNotNullOrEmpty(folderNameInput)) {
      String intermediateResult=folderNameInput;
      log.debug("EID-029 no substitution applied, result:"+folderNameInput);
      folderName=folderNameInput;
      intermediateResult=folderName;
      if(intermediateResult.contains(dateTag)) {
        DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
        String bpdfStr=bpdf.format(now);
        intermediateResult=intermediateResult.replace(dateTag, bpdfStr);
        folderName=intermediateResult;
        log.debug("EID-024 dateTag substitution applied, result:"+folderName);
      }
    }
    /*
     * Obtain the filename to write
     */
    String fileNameInput=args.getString("filename");
    // Start with the default filename 
    String intermediateFilename=defaultFilename;
    // Write the selection to the logs
    log.debug("EID-021 filename:"+fileNameInput);
    // If the input is null then use the default.
    if(Util.isNotNullOrEmpty(fileNameInput)) {
      intermediateFilename=fileNameInput;
    }
    /*
     * Check the filename for $date$
     */
    if(intermediateFilename.contains(dateTag)) {
      DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
      String bpdfStr=bpdf.format(now);
      intermediateFilename=intermediateFilename.replace(dateTag, bpdfStr);
      log.debug("EID-025 dateTag substitution applied, result:"+intermediateFilename);
    }
    /*
     * Check the filename for $identityName$
     */
    if(intermediateFilename.contains(identityNameTag)) {
      log.debug("EID-022 Filename contains "+identityNameTag+", performing substitution");
      if(processIdentities.size()==1) {
        log.debug("EID-023 replacing one instance with "+identityName.replace(" ", ""));
        intermediateFilename=intermediateFilename.replace(identityNameTag, identityName.replace(" ", ""));
      }
      else {
        DateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmm");
        String nowStr=sdf.format(now);
        intermediateFilename=intermediateFilename.replace(identityNameTag, nowStr);
      }
    }
    /*
     * Check the filename for $attribute$ where attribute is an identity attribute
     * Or an account attribute ? Need link for that, might be difficult
     */
    String attributeTag="";
    String trimmedAttribute="";
    String attributeValue="";
    int indexOfFirstDollar=-1;
    int indexOfNextDollar=-1;
    if(processIdentities.size()==1) {
      String identId=processIdentities.get(0);
      Identity queryID=context.getObjectById(Identity.class,identId);
      log.debug("EID-061 Looking for $ in filename");
      while(intermediateFilename.contains("$")) {
        indexOfFirstDollar=intermediateFilename.indexOf("$");
        indexOfNextDollar=intermediateFilename.indexOf("$",(indexOfFirstDollar+1));
        log.debug("EID-062 Found $ at "+indexOfFirstDollar+" and "+indexOfNextDollar);
        if (indexOfNextDollar > indexOfFirstDollar) {
          attributeTag=intermediateFilename.substring(indexOfFirstDollar,indexOfNextDollar+1);
          trimmedAttribute=intermediateFilename.substring(indexOfFirstDollar+1,indexOfNextDollar);
          log.debug("EID-063 attributeTag="+attributeTag+" , attributeName="+trimmedAttribute);
          attributeValue=queryID.getStringAttribute(trimmedAttribute);
          if(Util.isNotNullOrEmpty(attributeValue)) {
            log.debug("EID-064 attributeValue="+attributeValue);
            intermediateFilename=intermediateFilename.replace(attributeTag, attributeValue);
          }
          else {
            log.debug("EID-065 attributeValue is null, removing dollar signs");
            intermediateFilename=intermediateFilename.replace(attributeTag, trimmedAttribute);
          }
        }
      }
      context.decache(queryID);
    }
    String fullpathName="";
    if(intermediateFilename.contains(".")) {
      fullpathName=folderName+File.separator+intermediateFilename;
    }
    else {
      fullpathName=folderName+File.separator+intermediateFilename+".csv";
    }
    log.debug("EID-110 fullpathName="+fullpathName);
    updateProgress(context, result, "Full pathname="+fullpathName);

    StringBuffer sbheader=new StringBuffer();
    StringBuffer sbvalue=new StringBuffer();
    int numlines=0;
    boolean headerCreated=false;
    int count=0;
    int linkCount=0;
    String identKey="";
    String identValue="";
    List<String> identAttrs=new ArrayList<String>();
    identKey="name";
    ObjectConfig identityConfig=ObjectConfig.getObjectConfig(Identity.class);
    List<ObjectAttribute> identityObjectAttrs=identityConfig.getObjectAttributes();
    for(ObjectAttribute oattr: identityObjectAttrs) {
      identAttrs.add(oattr.getName());
    }
    identAttrs.remove(identKey);
    /*
     * Remove OOTB Attributes
     */
    identAttrs.remove("bundles");
    identAttrs.remove("bundleSummary");
    identAttrs.remove("assignedRoles");
    identAttrs.remove("assignedRoleSummary");
    identAttrs.remove("exceptions");
    identAttrs.remove("lastRefresh");
    identAttrs.remove("lastLogin");
    identAttrs.remove("capabilities");
    identAttrs.remove("scorecard.compositeScore");
    identAttrs.remove("managerStatus");
    identAttrs.remove("rights");
    identAttrs.remove("workgroups");
    identAttrs.remove("softwareVersion");
    identAttrs.remove("inactive");
    identAttrs.remove("password");
    Collections.sort(identAttrs);
    /**
     * Determine excludes
     */
    try {
      if(excludeListStr != null) {
        if(excludeListStr.contains(",")) {
          String[] excludeListArray = excludeListStr.split(",");
          excludeList.addAll(Arrays.asList(excludeListArray));
        }
        excludeList.add(excludeListStr);
      }
      if(includeListStr != null) {
        if(includeListStr.contains(",")) {
          String[] includeListArray = includeListStr.split(",");
          includeList.addAll(Arrays.asList(includeListArray));
        }
        includeList.add(includeListStr);
      }
    }
    catch (Exception ex) {
      log.error("EID-074 error parsing the exclude or include list");
    }
    for (String excludeStr: excludeList) {
      if(excludeStr.startsWith("type:")) {
        String excludeType = excludeStr.substring(5);
        log.debug("EID-075 Excluding attributes of type "+excludeType);
        for(ObjectAttribute oattr: identityObjectAttrs) {
          boolean excludeBool=false;
          SearchInputDefinition.PropertyType oattrType=oattr.getPropertyType();
          if("all".equalsIgnoreCase(excludeType)) {
            excludeBool=true;
          }
          else if("identity".equalsIgnoreCase(excludeType)) {
            if(oattrType == SearchInputDefinition.PropertyType.Identity) excludeBool=true;
          }
          else if("string".equalsIgnoreCase(excludeType)) {
            if(oattrType == SearchInputDefinition.PropertyType.String) excludeBool=true;
          }
          else if("date".equalsIgnoreCase(excludeType)) {
            if(oattrType == SearchInputDefinition.PropertyType.Date) excludeBool=true;
          }
          else if("boolean".equalsIgnoreCase(excludeType)) {
            if(oattrType == SearchInputDefinition.PropertyType.Boolean) excludeBool=true;
          }
          if(excludeBool) {
            identAttrs.remove(oattr.getName());
          }
        }
      }
      else {
        identAttrs.remove(excludeStr);
      }
    }
    for (String includeStr: includeList) {
      if(includeStr.startsWith("type:")) {
        String includeType = includeStr.substring(5);
        log.debug("EID-076 Including attributes of type "+includeType);
        for(ObjectAttribute oattr: identityObjectAttrs) {
          boolean includeBool=false;
          SearchInputDefinition.PropertyType oattrType=oattr.getPropertyType();
          if("all".equalsIgnoreCase(includeType)) {
            includeBool=true;
          }
          else if("identity".equalsIgnoreCase(includeType)) {
            if(oattrType == SearchInputDefinition.PropertyType.Identity) includeBool=true;
          }
          else if("string".equalsIgnoreCase(includeType)) {
            if(oattrType == SearchInputDefinition.PropertyType.String) includeBool=true;
          }
          else if("date".equalsIgnoreCase(includeType)) {
            if(oattrType == SearchInputDefinition.PropertyType.Date) includeBool=true;
          }
          else if("boolean".equalsIgnoreCase(includeType)) {
            if(oattrType == SearchInputDefinition.PropertyType.Boolean) includeBool=true;
          }
          if(includeBool) {
            identAttrs.add(oattr.getName());
          }
        }
      }
      else {
        identAttrs.add(includeStr);
      }
    }
    /**
     * Currently not going to attempt o back compute the attribute that is
     * used to correlate the identity's manager.  This is an account value
     * not an identity attribute.
     */
    log.debug("EID-111 identKey = "+identKey);
    log.debug("EID-112 attrs="+identAttrs);
    log.debug("EID-113 identities="+processIdentities.toString());
    
    updateProgress(context, result, "Writing file");
    
    try {
      File newFile = new File(fullpathName);
      if(newFile.createNewFile()) {
        log.debug("EID-057 file did not exist, creating new");
      }
      else {
        log.debug("EID-058 file existed, creating new");
      }
      PrintWriter outfile=new PrintWriter(newFile,"UTF-8");
      
      for (String iden2Id:processIdentities) {
        Identity iden2=context.getObjectById(Identity.class,iden2Id);
        if (iden2 != null) {
          log.debug("EID-114 working on identity="+iden2.getName());
        }
        else {
          log.debug("EID-115 NULL Identity.");
        }
        /*
         * Converting this code from dealing with accounts
         * into dealing with identities
         */
        if(!headerCreated) {
          int headercount=1;
          sbheader.append(identKey);
          for(String skey:identAttrs) {
            sbheader.append(",");
            sbheader.append(skey);
            headercount++;
            log.debug("EID-116 to the header, added :"+skey+" at location "+headercount);
          }
          // KCS 2020 Nov added to output disabled status
          sbheader.append(",correlated,inactive");
          log.debug("EID-055 header size="+headercount+" fields="+sbheader.toString());
          /*
           * Change made KCS 07/22/2023 to allow printing of the base header
           */
          if(printSchemaParams) {
            sboutput.append("\nIdentity Attribute list (name first):\n"+sbheader.toString()+"\n");
          }
          /*
           * Change made KCS 07/22/2023 to change what gets printed
           * literally use the input string, requires the first element to be
           * the nativeIdentity
           */
          if(schemaParamsList.isEmpty()) {
            outfile.println(sbheader.toString());
          }
          else {
            outfile.println(schemaParamsStr);
          }
          outfile.flush();
          headerCreated=true;
        }
        count=1;
        log.debug("EID-117 creating a new sbvalue StringBuffer");
        sbvalue=new StringBuffer();
        // Start with the identity Key
        // KCS 2020 Nov added quotes if contains commas or spaces
        identValue=qs(iden2);
        log.debug("EID-118 Found identity name = "+identValue);
        sbvalue.append(identValue);
        String svalue="";
        /*
         * Change made KCS 07/22/2023 to change what gets printed
         */
        if(!schemaParamsList.isEmpty()) {
          identAttrs.clear();
          identAttrs.addAll(schemaParamsList);
          identAttrs.remove(identKey);
        }
        for(String skey2:identAttrs) {
          if(skey2.equals("correlated")) {
            if(iden2.isCorrelated()) {
              sbvalue.append(",TRUE");
            }
            else {
              sbvalue.append(",FALSE");
            }
            continue;
          }
          if(skey2.equals("inactive")) {
            if(iden2.isInactive()) {
              sbvalue.append(",TRUE");
            }
            else {
              sbvalue.append(",FALSE");
            }
            continue;
          }
          sbvalue.append(",");
          svalue="";
          Object avalue=iden2.getAttribute(skey2);
          svalue=qs(avalue);
          sbvalue.append(svalue);
          count++;
          log.trace("EID-119 got attribute: "+skey2+" and value is "+svalue);
        }
        // Correlated
        if(schemaParamsList.isEmpty()) {
          if(iden2.isCorrelated()) {
            sbvalue.append(",TRUE");
          }
          else {
            sbvalue.append(",FALSE");
          }
          // Inactive
          if(iden2.isInactive()) {
            sbvalue.append(",TRUE");
          }
          else {
            sbvalue.append(",FALSE");
          }
        }
        log.debug("EID-056 value="+sbvalue.toString());
        outfile.println(sbvalue.toString());
        outfile.flush();
        numlines++;
        context.decache(iden2);
      }
      outfile.close();
      log.debug("EID-059 finished writing file");
      updateProgress(context, result, "Completed");
    }
    catch(Exception ex) {
      log.error("EID-099 "+ex.getClass().getName()+":"+ex.getMessage());
      result.setCompleted(new Date());
      result.setCompletionStatus(TaskResult.CompletionStatus.Error);
      result.setAttribute("resultString", sboutput.toString());
      context.saveObject(result);
      context.commitTransaction();
    }
    result.setAttribute("applications", applicationName);
    result.setAttribute("total", Integer.valueOf(numlines));
    result.setCompleted(new Date());
    result.setCompletionStatus(TaskResult.CompletionStatus.Success);
    result.setAttribute("resultString", sboutput.toString());
    context.saveObject(result);
    context.commitTransaction();
  }

  private String qs(Object avalue) {
    String svalue="";
    if(avalue==null)return svalue;
    if(avalue instanceof String) {
      svalue=(String)avalue;
      if(svalue.contains(",") || svalue.contains(" ") || svalue.contains(":") || svalue.contains("#") || svalue.contains("$")) {
        svalue="\""+svalue+"\"";
      }
    }
    else if (avalue instanceof Boolean) {
      svalue=((Boolean)avalue).toString();
    }
    else if(avalue instanceof List) {
      svalue=avalue.toString();
      svalue=svalue.substring(1);
      int lensvalue=svalue.length();
      svalue=svalue.substring(0,lensvalue-1);
      svalue="\""+svalue+"\"";
    }
    else if(avalue instanceof Identity) {
      svalue=((Identity)avalue).getName();
      if(svalue.contains(",") || svalue.contains(" ") || svalue.contains(":") || svalue.contains("#") || svalue.contains("$")) {
        svalue="\""+svalue+"\"";
      }
    }
    else {
      svalue=avalue.toString();
      if(svalue.contains(",") || svalue.contains(" ") || svalue.contains(":") || svalue.contains("#") || svalue.contains("$")) {
        svalue="\""+svalue+"\"";
      }
    }
    return svalue;
  }
  @Override
  public boolean terminate() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getPluginName() {
    // TODO Auto-generated method stub
    return "MCSpoofingPlugin";
  }
}
