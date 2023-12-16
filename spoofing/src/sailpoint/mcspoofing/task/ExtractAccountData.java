package sailpoint.mcspoofing.task;

//import org.apache.log4j.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Arrays;
import java.util.Date;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import sailpoint.api.SailPointContext;
import sailpoint.api.IdentityService;
import sailpoint.api.IncrementalObjectIterator;
import sailpoint.object.SailPointObject;
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
import sailpoint.task.AbstractTaskExecutor;
import sailpoint.task.BasePluginTaskExecutor;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;
import sailpoint.tools.Message;

/**
 * Extract data from any account for future aggregation.  This assumes all values are single strings
 * Have not accounted for multivalued strings.  Normally used for authoritative sources
 * @author keith
 */
public class ExtractAccountData extends BasePluginTaskExecutor {
  private static Log log = LogFactory.getLog(ExtractAccountData.class);
  
  private String applicationName="";
  private Application application=null;
  private List<String> processIdentities = new ArrayList<String>();
  private String folderName="";
  private String defaultFilename="$applicationName$-$identityName$";
  private Boolean getAllAccounts=false;
  private Boolean onlyActiveIdentities=true;
  private Boolean onlyCorrelatedIdentities=true;
  private Boolean printSchemaParams=false;
  private List<String> schemaParamsList=new ArrayList<String>();
  private String schemaParamsStr="";
  private long oldestDate=0L;
  private StringBuffer sboutput=new StringBuffer();
  private TaskResult taskResult=null;
  
  @SuppressWarnings({"rawtypes","unchecked"})
  @Override
  public void execute(SailPointContext context, TaskSchedule schedule, TaskResult result, Attributes<String, Object> args)
    throws Exception {
    log.debug("EAD-001 Starting Extract Account Data task");
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
    sboutput.append("ExtractAccountData started at "+sdfout.format(now)+"\n");
    
    String folderSep=File.separator;
    if(folderSep.equals("/")) {
      folderName="~"+folderSep+"extractdata";
    }
    else {
      folderName="C:"+folderSep+"ExtractData";
    }
    log.debug("EAD-002 initial folderName="+folderName);
    taskResult = result;
    // Get arguments
    if(args==null || args.isEmpty()) {
      log.warn("EAD-003 args is null or empty, exiting, need at least one application");
      result.setCompleted(new Date());
      result.setCompletionStatus(TaskResult.CompletionStatus.Error);
      result.setAttribute("resultString", sboutput.toString());
      context.saveObject(result);
      context.commitTransaction();
      return;
    }
    List applications = args.getList("applications");
    log.debug("EAD-004 List of applications="+applications.toString());
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
    List identities=new ArrayList();
    if(args.containsKey("identities")) {
      identities = args.getList("identities");
      log.debug("EAD-005 Identities List:" + identities.toString());
    }
    String identityName = "";
    Identity identity=null;
    String applicationNameTag="$applicationName$";
    String identityNameTag="$identityName$";
    String dateTag="$date$";
    result.put("internalStartDate", now);

    for(Object app: applications) {
      log.debug("EAD-006 Application class:"+app.getClass().getName());
      if(app instanceof String) {
        applicationName = (String)app;
        log.debug("EAD-007 Application selected:"+applicationName);
        application = context.getObjectByName(Application.class, applicationName);
      }
    }
    updateProgress(context, result, "Application selected: "+applicationName);
    
    if(identities!=null && !identities.isEmpty()) {
      for(Object ident: identities) {
        log.debug("EAD-008 Identity class:"+ident.getClass().getName());
        if(ident instanceof String) {
          if ( ((String)ident).contains(",") ) {
            for( String workingIdentity : ((String)ident).split( "," ) ) {
              identityName = ((String)workingIdentity).trim();
              log.debug("EAD-009 Identity selected:"+identityName);
              identity = context.getObjectByName(Identity.class, identityName);
              if (identity != null ) {
                log.debug("EAD-010 identityName="+identity.getName());
              }
              else {
                log.debug("EAD-011 Null Identity Returned for: " + identityName);
              }
              processIdentities.add(identity.getId());
              context.decache(identity);
            }
          }
          else {
            identityName = (String)ident;
            log.debug("EAD-012 Identity selected:"+identityName);
            identity = context.getObjectByName(Identity.class, identityName);
            if (identity != null ) {
              log.debug("EAD-013 identityName="+identity.getName());
            }
            else {
              log.debug("EAD-014 Null Identity Returned for: " + identityName);
            }
            processIdentities.add(identity.getId());
            context.decache(identity);
          }
        }
      }
    }
    log.debug("EAD-015 processIdentities after update="+processIdentities.toString());
    if(processIdentities.size()>0) {
      if(processIdentities.size()==1) {
        updateProgress(context, result, "Identity selected: "+identityName);
      }
      else {
        updateProgress(context, result, "Multiple Identities selected");
      }
    }
    log.debug("EAD-016 identities="+processIdentities.toString());
    IdentityService idService=new IdentityService(context);
    getAllAccounts=args.getBoolean("selectall");
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
    if(getAllAccounts.booleanValue()) {
      log.debug("EAD-031 getAllAccounts is true, getting all users");
      QueryOptions qo1=new QueryOptions();
      List<Filter> filterList=new ArrayList<Filter>();
      Filter appFilter=Filter.eq("links.application.name",applicationName);
      filterList.add(appFilter);
      /*
       * If onlyActiveIdentities, add that filter
       */
      if(onlyActiveIdentities.booleanValue()) {
        Filter activeFilter=Filter.eq("inactive", false);
        filterList.add(activeFilter);
        log.debug("EAD-046 added filter for inactive=false");
      }
      /*
       * If onlyCorrelatedIdentities, add that filter
       */
      if(onlyCorrelatedIdentities.booleanValue()) {
        Filter correlatedFilter=Filter.eq("correlated", true);
        filterList.add(correlatedFilter);
        log.debug("EAD-047 added filter for correlated=true");
      }
      if(filterList.size()==1) {
        qo1.addFilter(appFilter);
      }
      else {
        qo1.addFilter(Filter.and(filterList));
      }
      //Iterator it = context.search(Identity.class,qo1);
      IncrementalObjectIterator it=new IncrementalObjectIterator(context,Identity.class,qo1);
      while(it.hasNext()) {
        Identity hasAccount = (Identity)it.next();
        log.debug("EAD-032 Identity:"+hasAccount.getName()+" has an account");
        processIdentities.add(hasAccount.getId());
        context.decache(hasAccount);
      }
      Integer sizeInt=Integer.valueOf(processIdentities.size());
      updateProgress(context, result, "All accounts selected, count="+sizeInt.toString());
      log.debug("EAD-033 All accounts selected, count="+sizeInt.toString());
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
    log.debug("EAD-0018 folderpath:"+folderNameInput);
    if(Util.isNotNullOrEmpty(folderNameInput)) {
      String intermediateResult=folderNameInput;
      if(intermediateResult.contains(applicationNameTag)) {
        intermediateResult=folderNameInput.replace(applicationNameTag, applicationName.replace(" ",""));
        log.debug("EAD-019 substitution applied, intermediate result:"+intermediateResult);
        folderName=intermediateResult;
      }
      else {
        log.debug("EAD-020 no substitution applied, result:"+folderNameInput);
        folderName=folderNameInput;
      }
      intermediateResult=folderName;
      if(intermediateResult.contains(dateTag)) {
        DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
        String bpdfStr=bpdf.format(now);
        intermediateResult=intermediateResult.replace(dateTag, bpdfStr);
        folderName=intermediateResult;
        log.debug("EAD-024 dateTag substitution applied, result:"+folderName);
      }
    }
    // Check for existence of basePath
    File basePathObj=new File(folderName);
    if(basePathObj.exists()) {
      log.debug("The folderName "+folderName+" exists");
    }
    else {
      if(basePathObj.mkdirs()) {
        log.debug("Successfully created "+folderName);
      }
      else {
        log.error("Count not create folder "+folderName);
        taskResult.setCompletionStatus(TaskResult.CompletionStatus.Error);
        taskResult.addMessage(new Message(Message.Type.Error,"Could not create output folder"));
        result.setAttribute("resultString", sboutput.toString());
        context.saveObject(result);
        context.commitTransaction();
        return;
      }
    }

    
    
    /*
     * Obtain the filename to write
     */
    String fileNameInput=args.getString("filename");
    // Start with the default filename 
    String intermediateFilename=defaultFilename;
    // Write the selection to the logs
    log.debug("EAD-021 filename:"+fileNameInput);
    // If the input is null then use the default.
    if(Util.isNotNullOrEmpty(fileNameInput)) {
      intermediateFilename=fileNameInput;
    }
    /*
     * Check the filename for $applicationName$
     */
    if(intermediateFilename.contains(applicationNameTag)) {
      intermediateFilename=intermediateFilename.replace(applicationNameTag, applicationName.replace(" ",""));
    }
    /*
     * Check the filename for $date$
     */
    if(intermediateFilename.contains(dateTag)) {
      DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
      String bpdfStr=bpdf.format(now);
      intermediateFilename=intermediateFilename.replace(dateTag, bpdfStr);
      log.debug("EAD-025 dateTag substitution applied, result:"+intermediateFilename);
    }
    /*
     * Check the filename for $identityName$
     */
    if(intermediateFilename.contains(identityNameTag)) {
      log.debug("EAD-022 Filename contains "+identityNameTag+", performing substitution");
      if(processIdentities.size()==1) {
        log.debug("EAD-023 replacing one instance with "+identityName.replace(" ", ""));
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
      log.debug("EAD-041 Looking for $ in filename");
      while(intermediateFilename.contains("$")) {
        indexOfFirstDollar=intermediateFilename.indexOf("$");
        indexOfNextDollar=intermediateFilename.indexOf("$",(indexOfFirstDollar+1));
        log.debug("EAD-042 Found $ at "+indexOfFirstDollar+" and "+indexOfNextDollar);
        if (indexOfNextDollar > indexOfFirstDollar) {
          attributeTag=intermediateFilename.substring(indexOfFirstDollar,indexOfNextDollar+1);
          trimmedAttribute=intermediateFilename.substring(indexOfFirstDollar+1,indexOfNextDollar);
          log.debug("EAD-043 attributeTag="+attributeTag+" , attributeName="+trimmedAttribute);
          attributeValue=queryID.getStringAttribute(trimmedAttribute);
          if(Util.isNotNullOrEmpty(attributeValue)) {
            log.debug("EAD-043 attributeValue="+attributeValue);
            intermediateFilename=intermediateFilename.replace(attributeTag, attributeValue);
          }
          else {
            log.debug("EAD-044 attributeValue is null, removing dollar signs");
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
    log.debug("EAD-110 fullpathName="+fullpathName);
    updateProgress(context, result, "Full pathname="+fullpathName);

    StringBuffer sbheader=new StringBuffer();
    StringBuffer sbvalue=new StringBuffer();
    int numlines=0;
    List<String> processStrings=new ArrayList<String>();
    boolean headerCreated=false;
    int count=0;
    int linkCount=0;
    String identKey="";
    List<String> identAttrs=new ArrayList<String>();
    Schema accountSchema=application.getSchema("account");
    identKey=accountSchema.getIdentityAttribute();
    identAttrs.addAll(accountSchema.getAttributeNames());
    identAttrs.remove(identKey);
    log.debug("EAD-111 identKey = "+identKey);
    log.debug("EAD-112 attrs="+identAttrs);
    log.debug("EAD-112.1 identities="+processIdentities.toString());
    
    updateProgress(context, result, "Writing file");
    PrintWriter outfile=null;
    try {
      File newFile = new File(fullpathName);
      if(newFile.createNewFile()) {
        log.debug("EAD-057 file did not exist, creating new");
      }
      else {
        log.debug("EAD-058 file existed, creating new");
      }
      outfile=new PrintWriter(newFile,"UTF-8");
      
      for (String iden2Id:processIdentities) {
        Identity iden2=context.getObjectById(Identity.class,iden2Id);
        if (iden2 != null) {
          log.debug("EAD-112.2 working on identity="+iden2.getName());
        }
        else {
          log.debug("EAD-112.3 NULL Identity.");
        }
        List appLinks = idService.getLinks(iden2,application);
        linkCount=0;
        for (Object appLink: appLinks) {
          linkCount++;
          if(linkCount>1) {
            log.debug("EAD-113 linkCount ="+linkCount+" skipping");
            continue;
          }
          Attributes attrs=((Link)appLink).getAttributes();
          String identValue=((Link)appLink).getNativeIdentity();
          log.debug("EAD-054 nativeIdentity "+identKey+"="+identValue);
          // KCS 2020 Nov added to respond to basically null values
          if(attrs==null || attrs.isEmpty()) {
            log.debug("EAD-060 attrs is null or empty, skipping");
            continue;
          }
          List attrKeys=attrs.getKeys();
          Map attrMap=attrs.getMap();
          if(!headerCreated) {
            count=1;
            sbheader.append(identKey);
            for(String skey:identAttrs) {
              sbheader.append(",");
              sbheader.append(skey);
              count++;
            }
            // KCS 2020 Nov added to output disabled status
            sbheader.append(",IIQDisabled");
            log.debug("EAD-055 header="+sbheader.toString());
            /*
             * Change made KCS 07/22/2023 to allow printing of the base header
             */
            if(printSchemaParams) {
              sboutput.append("\nParameter list (nativeIdentity first):\n"+sbheader.toString()+"\n");
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
            headerCreated=true;
          }
          count=1;
          sbvalue=new StringBuffer();
          // Start with the identity Key
          // KCS 2020 Nov added quotes if contains commas or spaces
          if(identValue.contains(",") || identValue.contains(" ")) {
            identValue="\""+identValue+"\"";
          }
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
            if(skey2.equals("IIQDisabled")) {
              if( ((Link)appLink).isDisabled() ) {
                sbvalue.append(",true");
              }
              else {
                sbvalue.append(",false");
              }
              continue;
            }
            sbvalue.append(",");
            svalue="";
            if(attrMap.containsKey(skey2)) {
              Object avalue=attrMap.get(skey2);
              if (avalue instanceof Boolean) {
                svalue=((Boolean)avalue).toString();
              }
              else if(avalue instanceof String) {
                svalue=(String)avalue;
                if(svalue.contains("\"")) {
                  svalue=svalue.replace("\""," ");
                }
                if(svalue.contains(",") || svalue.contains(" ")) {
                  svalue="\""+svalue+"\"";
                }
              }
              else if(avalue instanceof List) {
                svalue=avalue.toString();
                svalue=svalue.substring(1);
                int lensvalue=svalue.length();
                svalue=svalue.substring(0,lensvalue-1);
                if(svalue.contains("\"")) {
                  svalue=svalue.replace("\""," ");
                }
                svalue="\""+svalue+"\"";
              }
              else if(avalue instanceof SailPointObject) {
                try {
                  svalue=((SailPointObject)avalue).getName();
                }
                catch (Exception exs) {
                  svalue=avalue.toString();
                }
              }
            }
            sbvalue.append(svalue);
            count++;
          }
          // KCS 2020 Nov added disabled state
          if(schemaParamsList.isEmpty()) {
            if( ((Link)appLink).isDisabled() ) {
              sbvalue.append(",true");
            }
            else {
              sbvalue.append(",false");
            }
          }
          log.debug("EAD-056 value="+sbvalue.toString());
          outfile.println(sbvalue.toString());
          outfile.flush();
          numlines++;
        }
        context.decache();
      }
    }
    catch(Exception ex) {
      log.error("EAD-099 "+ex.getClass().getName()+":"+ex.getMessage());
      result.setCompleted(new Date());
      result.setCompletionStatus(TaskResult.CompletionStatus.Error);
      result.setAttribute("resultString", sboutput.toString());
      context.saveObject(result);
      context.commitTransaction();
    }
    finally {
      try {
        outfile.close();
      }
      catch (Exception ex2) {
        log.error("EAD-097 "+ex2.getClass().getName()+":"+ex2.getMessage());
      }
      log.debug("EAD-059 finished writing file");
      updateProgress(context, result, "Completed");
    }
    result.setAttribute("applications", applicationName);
    result.setAttribute("total", Integer.valueOf(numlines));
    result.setAttribute("resultString", sboutput.toString());
    result.setCompleted(new Date());
    result.setCompletionStatus(TaskResult.CompletionStatus.Success);
    context.saveObject(result);
    context.commitTransaction();
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
