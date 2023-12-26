package sailpoint.mcspoofing.task;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import sailpoint.api.SailPointContext;
import sailpoint.api.IdentityService;
import sailpoint.api.Aggregator;
import sailpoint.connector.Connector;
import sailpoint.task.BasePluginTaskExecutor;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;
import sailpoint.object.Application;
import sailpoint.object.Attributes;
import sailpoint.object.Identity;
import sailpoint.object.ResourceObject;
import sailpoint.object.Link;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskResult.CompletionStatus;
import sailpoint.object.TaskSchedule;
import sailpoint.tools.Message;
import sailpoint.tools.Message.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Change Log
 *
 * @author Keith Smith
 */
public class TargetAggregation extends BasePluginTaskExecutor {
  private static final String PLUGIN_NAME = "MCSpoofingPlugin";
  private static Log log = LogFactory.getLog(TargetAggregation.class);
  private boolean terminate=false;
  private String sailpointVersion="";
  private String sailpointPatch="";
  private StringBuffer sboutput=new StringBuffer();
  private long oldestDate=0L;
  private String appName="";
  private TaskResult taskResult;
  @SuppressWarnings({"rawtypes","unchecked"})
  @Override
  public void execute(SailPointContext context, TaskSchedule schedule,
    TaskResult result, Attributes args) throws Exception {
    log.debug("TAG-001 TargetAggregation started");
    DateFormat sdfout=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    DateFormat sdfnotime=new SimpleDateFormat("MM/dd/yyyy");
    Date now=new Date();
    Date nowNoTime=now;
    sailpointVersion=sailpoint.Version.getVersion();
    sailpointPatch=sailpoint.Version.getPatchLevel();
    String nowDateNoTimeStr=sdfnotime.format(now);
    try {
      nowNoTime=sdfnotime.parse(nowDateNoTimeStr);
    }
    catch (Exception et) {}
    oldestDate=nowNoTime.getTime();
    sboutput.append("TargetAggregation started at "+sdfout.format(now)+"\n");

    List applications = args.getList("applications");
    log.debug("TAG-003 List of applications="+applications.toString());
    if(applications==null || applications.isEmpty()) {
      result.setAttribute("resultString", sboutput.toString());
      result.setCompletionStatus(TaskResult.CompletionStatus.Error);
      result.addMessage(new Message(Message.Type.Error,"No application specified"));
      context.saveObject(result);
      context.commitTransaction();
      return;
    }
    for(Object app:applications) {
      if(app instanceof String) {
        appName=(String)app;
      }
    }
    sboutput.append("\nApplication selected:"+appName);
    log.debug("TAG-004 Application selected:"+appName);
    
    String identityName="";
    List identities=new ArrayList();
    if(args.containsKey("identities")) {
      identities = args.getList("identities");
      log.debug("TAG-005 Identities List:" + identities.toString());
    }
    if(!identities.isEmpty()) {
      for(Object iden:identities) {
        if(iden instanceof String) {
          identityName=(String)iden;
        }
      }
    }
    Identity identObj=null;
    Application appObj=null;
    appObj=context.getObjectByName(Application.class,appName);
    String nativeIdentity="";
    if(args.containsKey("nativeIdentity")) {
      nativeIdentity=args.getString("nativeIdentity");
      log.debug("TAG-006 nativeIdentity:" + nativeIdentity);
    }
    
    Boolean noOptimizeReaggregation=args.getBoolean("noOptimizeReaggregation");
    log.debug("TAG-010 noOptimizeReaggregation="+noOptimizeReaggregation);
    if(Util.isNotNullOrEmpty(nativeIdentity)) {
      sboutput.append("\nnativeIdentity="+nativeIdentity);
      if(Util.isNotNullOrEmpty(identityName)) {
        log.debug("TAG-007 nativeIdentity is set so identity will be ignored");
        sboutput.append("\nnativeIdentity is set so identity will be ignored");
      }
    }
    else {
      sboutput.append("\nScanning identity "+identityName+" for accounts on "+appName);
      log.debug("TAG-008 Scanning identity "+identityName+" for accounts on "+appName);
      IdentityService idService=new IdentityService(context);
      identObj=context.getObjectByName(Identity.class,identityName);
      if(identObj==null) {
        result.setAttribute("resultString", sboutput.toString());
        result.setCompleted(new Date());
        result.setCompletionStatus(TaskResult.CompletionStatus.Error);
        result.addMessage(new Message(Message.Type.Error,"Identity "+identityName+" not found"));
        context.saveObject(result);
        context.commitTransaction();
        return;
      }
      if(appObj==null) {
        result.setAttribute("resultString", sboutput.toString());
        result.setCompleted(new Date());
        result.setCompletionStatus(TaskResult.CompletionStatus.Error);
        result.addMessage(new Message(Message.Type.Error,"Application "+appName+" not found"));
        context.saveObject(result);
        context.commitTransaction();
        return;
      }
      int numLinks=idService.countLinks(identObj,appObj);
      if(numLinks==0) {
        result.setAttribute("resultString", sboutput.toString());
        result.setCompleted(new Date());
        result.setCompletionStatus(TaskResult.CompletionStatus.Error);
        result.addMessage(new Message(Message.Type.Error,"User has no account on application "+appName));
        context.saveObject(result);
        context.commitTransaction();
        return;
      }
      else if(numLinks>1) {
        result.setAttribute("resultString", sboutput.toString());
        result.setCompleted(new Date());
        result.setCompletionStatus(TaskResult.CompletionStatus.Error);
        result.addMessage(new Message(Message.Type.Error,"User has multiple accounts, must specify the nativeIdentity"));
        context.saveObject(result);
        context.commitTransaction();
        return;
      }
      List<Link> appLinks=idService.getLinks(identObj,appObj);
      Link appLink=null;
      for(Link appLinkx:appLinks) {
        appLink=appLinkx;
      }
      nativeIdentity=appLink.getNativeIdentity();
      sboutput.append("\nnativeIdentity="+nativeIdentity);
    }
    int total_processed=0;
    int total_optimized=0;
    int total_updates=0;
    Connector appConn=sailpoint.connector.ConnectorFactory.getConnector(appObj,null);
    ResourceObject rObj=appConn.getObject("account",nativeIdentity,null);
    if(rObj!=null) {
      String rObjStr=rObj.toXml();
      rObjStr=rObjStr.replace("<","[").replace(">","]");
      sboutput.append("\nResourceObject returned:\n"+rObjStr);
      Attributes argMap=new Attributes();
      argMap.put("aggregationType","account");
      argMap.put("applications",appName);
      argMap.put("noOptimizeReaggregation",noOptimizeReaggregation.toString().trim().toLowerCase());
    //
    // For groups:
    // argMap.put("descriptionAttribute","description");
    // argMap.put("descriptionLocale","en_US");
      Aggregator agg=new Aggregator(context,argMap);
      TaskResult aggResult=agg.aggregate(appObj,rObj);
      if(aggResult.hasErrors()) {
        sboutput.append("\nResult has errors");
      }
      Attributes ratt=aggResult.getAttributes();
      Map ratmap=ratt.getMap();
      total_processed+=getIntegerValue(ratmap,"total");
      total_optimized+=getIntegerValue(ratmap,"optimized");
      total_updates+=getIntegerValue(ratmap,"internalUpdates");
    }
    sboutput.append("\n");
    result.setAttribute("applications", appName);
    result.setAttribute("total", Integer.valueOf(total_processed));
    result.setAttribute("optimized", Integer.valueOf(total_optimized));
    result.setAttribute("internalUpdates", Integer.valueOf(total_updates));
    result.setCompleted(new Date());
    result.setAttribute("resultString", sboutput.toString());
    result.setCompletionStatus(TaskResult.CompletionStatus.Success);
    result.addMessage(new Message(Message.Type.Info,"Processed"));
    context.saveObject(result);
    context.commitTransaction();
    log.debug("TAG-112 exiting");
    return;
  }
  public boolean terminate() {
    terminate=true;
    taskResult.setTerminated(true);
    if (log.isDebugEnabled())
      log.debug("Task was terminated."); 
    return true;
  }
  private int getIntegerValue(Map ratmap, String key) {
    int rval=0;
    if(ratmap.containsKey(key)) {
      Object ovalue=ratmap.get(key);
      if(ovalue instanceof String) {
        String svalue=(String)ovalue;
        try {
          Integer ivalue=Integer.parseInt(svalue);
          rval=ivalue.intValue();
        }
        catch (NumberFormatException ex) {
          log.error("AAD-901 "+svalue+" "+ex.getMessage());
        }
      }
      else if(ovalue instanceof Integer) {
        rval=((Integer)ovalue).intValue();
      }
    }
    return rval;
  }

  public String getPluginName() {
    return PLUGIN_NAME;
  }
}