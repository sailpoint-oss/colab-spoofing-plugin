package sailpoint.mcspoofing.task;

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

import sailpoint.connector.DelimitedFileConnector;

import sailpoint.api.Aggregator;
import sailpoint.api.SailPointContext;
import sailpoint.api.IdentityService;

import sailpoint.object.Attributes;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskSchedule;
import sailpoint.object.Application;
import sailpoint.object.Identity;
import sailpoint.object.Link;
import sailpoint.object.QueryOptions;
import sailpoint.object.Filter;
import sailpoint.object.ResourceObject;
import sailpoint.object.Rule;

import sailpoint.task.AbstractTaskExecutor;
import sailpoint.task.BasePluginTaskExecutor;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;
import sailpoint.tools.CloseableIterator;
/**
 * Aggregate extracted data from any account.  This assumes all values are single strings
 * Have not accounted for multivalued strings yet.  Also generally assumed for authoritative sources
 * 
 * @author keith smith
 *
 */
public class AggregateAccountData extends BasePluginTaskExecutor {
	private static Log log = LogFactory.getLog(AggregateAccountData.class);
	
	private String applicationName;
	private Application application;
	private String folderName="";
	private String defaultFilename="$applicationName$";
	
	private TaskResult taskResult;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void execute(SailPointContext context, TaskSchedule schedule, TaskResult result, Attributes<String, Object> args)
		throws Exception {
		
		log.debug("AAD-001 Starting Aggregate Account Data task");
		String folderSep=File.separator;
		if(folderSep.equals("/")) {
			folderName="~"+folderSep+"extractdata";
		}
		else {
			folderName="C:"+folderSep+"ExtractData";
		}
		taskResult = result;
		// Get arguments
		List applications = args.getList("applications");
		String identityName = "";
		Identity identity=null;
		String applicationNameTag="$applicationName$";
		Date now = new Date();
		result.put("internalStartDate", now);

		for(Object app: applications) {
			log.debug("AAD-002 Application class:"+app.getClass().getName());
			if(app instanceof String) {
				applicationName = (String)app;
				log.debug("AAD-003 Application selected:"+applicationName);
				application = context.getObjectByName(Application.class, applicationName);
			}
		}
		updateProgress(context, result, "Application selected: "+applicationName);

		IdentityService idService=new IdentityService(context); 
		/*
		 * Get the folder name
		 */
		String folderNameInput=args.getString("folderpath");
		log.debug("AAD-007 folderpath:"+folderNameInput);
		if(Util.isNotNullOrEmpty(folderNameInput)) {
			String intermediateResult=folderNameInput;
			if(intermediateResult.contains(applicationNameTag)) {
				intermediateResult=folderNameInput.replace(applicationNameTag, applicationName.replace(" ",""));
				log.debug("AAD-008 substitution applied, intermediate result:"+intermediateResult);
				folderName=intermediateResult;
			}
			else {
				log.debug("AAD-008 no substitution applied, result:"+folderNameInput);
				folderName=folderNameInput;
			}
		}
		
		/*
		 * Obtain the filename to read
		 */
		String fileNameInput=args.getString("filename");
		// Start with the default filename 
		String intermediateFilename=defaultFilename;
		// Write the selection to the logs
		log.debug("AAD-009 filename:"+fileNameInput);
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
		
		String fullpathName="";
		if(intermediateFilename.contains(".")) {
			fullpathName=folderName+File.separator+intermediateFilename;
		}
		else {
			fullpathName=folderName+File.separator+intermediateFilename+".csv";
		}
		log.debug("AAD-010 fullpathName="+fullpathName);
		updateProgress(context, result, "Full pathname="+fullpathName);

		/*
		 * Spoof the application
		 */
		Application delimitedProxy = new Application();
		Application simulationTarget = context.getObject(Application.class, applicationName);
		delimitedProxy.addSchema(simulationTarget.getAccountSchema());
		delimitedProxy.setAttribute("file", fullpathName);
		delimitedProxy.setAttribute("delimiter", ",");
		delimitedProxy.setAttribute("hasHeader", true);
		DelimitedFileConnector dfc = new DelimitedFileConnector(delimitedProxy);
		Attributes dfc_attrs=dfc.getAttributes();
		log.debug("AAD-023 Adding dontTransformCsvsToListAttrs=true");
		dfc_attrs.put("dontTransformCsvsToListAttrs",true);
		log.debug("AAD-024 Adding mergeRows=false");
		dfc_attrs.put("mergeRows",false);
		log.debug("AAD-020 Adding filetransport=local");
		dfc_attrs.put("filetransport","local");
		log.debug("AAD-021 Adding filterEmptyRecords=true");
		dfc_attrs.put("filterEmptyRecords",true);
		log.debug("AAD-022 Adding commentCharacter=#");
		dfc_attrs.put("commentCharacter","#");
		dfc.setAttributes(dfc_attrs);
		/*
		 * Pull the data from the file NOW
		 */
		CloseableIterator it = dfc.iterateObjects("account", null, null);
		updateProgress(context, result, "Read the file, iterating through accounts");
		
		List<ResourceObject> testingResourceObjects = new ArrayList<ResourceObject>();
		Map state = new HashMap();
		log.debug("AAD-011 Iterate over resource objects from delimited file connector");
		
		while (it.hasNext()) {
			ResourceObject ro = (ResourceObject)it.next();
			log.debug("AAD-012 Iterating next resource object for attribute customization: " + ro.getIdentity());
			//Need to do this because for some reason, Department is multivalued in Workday Schema but Identity attribute rule isn't smart enough to handle it :/
			if (applicationName.contains("Workday Direct")) {
				java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("MM/dd/yy hh:mm aa");
				/* Here are some examples of manipulation
				Object dept = ro.get("Department");
				if (dept != null && dept instanceof List) {
					dept = dept.get(0);
					ro.setAttribute("Department",dept);
				}
				Object deptCode = ro.get("DepartmentCode");
				if (deptCode != null && deptCode instanceof List) {
					deptCode = deptCode.get(0);
					ro.setAttribute("DepartmentCode",deptCode);
				}

				Object term = ro.get("IsEmployeeTerminated");
				ro.setAttribute("IsEmployeeTerminated",sailpoint.tools.Util.otob(term));

				String termdate = (String) ro.get("TerminationDate");
				if (termdate != null) ro.setAttribute("TerminationDate",sdf.parse(termdate));

				String hiredate = (String) ro.get("HIREDATE");
				if (hiredate != null) {
					Date hdate = sdf.parse(hiredate);
					//long timeInMillis = hdate.getTime();
					ro.setAttribute("HIREDATE",hdate);
				}

				Object leave = ro.get("OnLeave");
				ro.setAttribute("OnLeave",sailpoint.tools.Util.otob(leave));
				 */
			}
			// JKG - Added for multivalued attribute SupervisoryOrganizations
			// TODO Convert to method to use on multiple multivalued attributes.
			if (applicationName.contains("Workday TMNA")) {
				log.debug("AAD-025 Found applicationName = "+applicationName);
				Object supOrgs = ro.get("SupervisoryOrganizations");
				if(supOrgs==null) {
					log.debug("AAD-026 supOrgs is null, skipping");
				}
				else {
					log.debug("AAD-012.1 SupervisoryOrganizations class:"+supOrgs.getClass().getName());
					if (supOrgs instanceof List) {
						List<String> supOrgsList = new ArrayList<String>();
						// Account exporter is leaving [ and ] characters on the list.  Remove them.
						for ( Object supOrg : (List)supOrgs ) {
							if (((String)supOrg).toString().contains("[")) {
								supOrg = ((String)supOrg).toString().replace("[", "");
								log.debug("AAD-012.2 Stripped [ character:" + ((String)supOrg).toString());
							}
							if (((String)supOrg).toString().contains("]")) {
								supOrg = ((String)supOrg).toString().replace("]", "");
								log.debug("AAD-012.3 Stripped ] character:" + ((String)supOrg).toString());
							}
							supOrgsList.add(((String)supOrg).toString());
						}
						ro.setAttribute("SupervisoryOrganizations",supOrgsList);
						log.debug("AAD-012.4 List: SupervisoryOrganizations:" + supOrgsList.toString());
					}
				}
			}

			//Run customization if configured on the application
			Rule customizationRule = simulationTarget.getCustomizationRule();
			if(customizationRule != null) {
				log.debug("AAD-013 Customization Rule: " + customizationRule);
			}
			/*
			 * KCS 2020-05-08 Start of fix for customizationRule issue
			 */
			Rule accountCustomizationRule=simulationTarget.getCustomizationRule("account");
			if(accountCustomizationRule != null) {
				log.debug("AAD-013 Account Customization Rule: "+accountCustomizationRule.getName());
				customizationRule=accountCustomizationRule;
			}
			// KCS 2020-05-08 end of fix
			if (customizationRule != null) {
				// Pass the mandatory arguments to the Customization rule for the app.
				HashMap ruleArgs = new HashMap();
				ruleArgs.put("context",context);
				ruleArgs.put("log",log);
				ruleArgs.put("object",ro);
				ruleArgs.put("application",simulationTarget);
				ruleArgs.put("connector",sailpoint.connector.ConnectorFactory.getConnector(simulationTarget, null));
				ruleArgs.put("state",state);
				// Call the customization rule 
				ro=(ResourceObject) context.runRule(customizationRule, ruleArgs, null);
			}

			if(ro != null) {
				testingResourceObjects.add(ro);
				log.debug("AAD-014 Resouce object XML:");
				log.debug("AAD-015 "+ro.toXml());
			}
		}

		//Call the aggregator to aggregate the objects
		Attributes aggregationArgs = new Attributes();
		Boolean noOptimizeReaggregation=args.getBoolean("noOptimizeReaggregation");
		
		aggregationArgs.put("noOptimizeReaggregation",noOptimizeReaggregation);
		Aggregator aggregator = new Aggregator(context, aggregationArgs);

		//Aggregate single resource objects from list one by one because of issues with accounts not coming in with aggregateAccounts(Iterator)
		int total_processed=0;
		int total_optimized=0;
		int total_updates=0;
		for (ResourceObject rObj : testingResourceObjects) {
			log.debug("AAD-016 Aggregating object: " + rObj.getIdentity());
			updateProgress(context, result, "Aggregating object: "+rObj.getIdentity());
			TaskResult tr = aggregator.aggregate(simulationTarget, rObj);
			log.debug("AAD-017 " + tr.toXml());
			Attributes ratt=tr.getAttributes();
			Map ratmap=ratt.getMap();
			total_processed+=getIntegerValue(ratmap,"total");
			total_optimized+=getIntegerValue(ratmap,"optimized");
			total_updates+=getIntegerValue(ratmap,"internalUpdates");
		}

		// Formerly did the entire list in one swath AGgregate list of resource objects
		//TaskResult tr = aggregator.aggregate(simulationTarget, testingResourceObjects);
		//alog.debug("AAD-017 " + tr.toXml());

		log.debug("AAD-019 finished writing file");
		updateProgress(context, result, "Completed");
		result.setAttribute("applications", applicationName);
		result.setAttribute("total", Integer.valueOf(total_processed));
		result.setAttribute("optimized", Integer.valueOf(total_optimized));
		result.setAttribute("internalUpdates", Integer.valueOf(total_updates));
		result.setCompleted(new Date());
		result.setCompletionStatus(TaskResult.CompletionStatus.Success);
		context.saveObject(result);
		context.commitTransaction();
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
