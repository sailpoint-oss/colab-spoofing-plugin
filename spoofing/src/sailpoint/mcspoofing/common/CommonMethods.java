package sailpoint.mcspoofing.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
import java.util.zip.*;
import java.io.*;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sailpoint.api.SailPointContext;
import sailpoint.api.IncrementalObjectIterator;
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

public class CommonMethods {
  private static final String PLUGIN_NAME = "TasksPlugin";
  private static Log log = LogFactory.getLog(CommonMethods.class);
  public static Object getIdentityAttribute(Identity identity, String attrName) {
    try {
      if("name".equals(attrName)) {
        return identity.getName();
      }
      else if("displayName".equals(attrName)) {
        return identity.getDisplayName();
      }
      else if("email".equals(attrName)) {
        return identity.getEmail();
      }
      else if("firstname".equals(attrName)) {
        return identity.getFirstname();
      }
      else if("lastname".equals(attrName)) {
        return identity.getLastname();
      }
      else {
        return identity.getAttribute(attrName);
      }
    }
    catch (Exception ex) {
      log.error("CM-090 "+ex.getClass().getName()+":"+ex.getMessage());
      return null;
    }
  }
  public static String normalizeToString(Object obj) {
    if(obj==null) {
      return "";
    }
    else if(obj instanceof String) {
      return (String)obj;
    }
    else {
      return obj.getClass().getName()+":"+obj.toString();
    }
  }
  public static boolean isVersionGE(String currentVersion, String currentPatch, String requiredVersion, String requiredPatch) {
    Map<String,Integer> versionMap=new HashMap<String,Integer>();
    versionMap.put("7.0",Integer.valueOf(700));
    versionMap.put("7.1",Integer.valueOf(710));
    versionMap.put("7.2",Integer.valueOf(720));
    versionMap.put("7.3",Integer.valueOf(730));
    versionMap.put("8.0",Integer.valueOf(800));
    versionMap.put("8.1",Integer.valueOf(810));
    versionMap.put("8.2",Integer.valueOf(820));
    versionMap.put("8.3",Integer.valueOf(830));
    versionMap.put("8.4",Integer.valueOf(840));
    Map<String,Integer> patchMap=new HashMap<String,Integer>();
    patchMap.put("p1",Integer.valueOf(1));
    patchMap.put("p2",Integer.valueOf(2));
    patchMap.put("p3",Integer.valueOf(3));
    patchMap.put("p4",Integer.valueOf(4));
    patchMap.put("p5",Integer.valueOf(5));
    patchMap.put("p6",Integer.valueOf(6));
    patchMap.put("p7",Integer.valueOf(7));
    patchMap.put("p8",Integer.valueOf(8));
    patchMap.put("p9",Integer.valueOf(9));
    int currentVersionInt=(versionMap.get(currentVersion)).intValue();
    int currentPatchInt=Util.isNullOrEmpty(currentPatch)?0:(patchMap.get(currentPatch)).intValue();
    int requiredVersionInt=(versionMap.get(requiredVersion)).intValue();
    int requiredPatchInt=Util.isNullOrEmpty(requiredPatch)?0:(patchMap.get(requiredPatch)).intValue();
    int currentVP=currentVersionInt+currentPatchInt;
    int requiredVP=requiredVersionInt+requiredPatchInt;
    return (currentVP >= requiredVP);
  }
}
