package enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.statistics.stats01;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.estatistics.bhdpscorestats.logics.*;

public class Stats01BL extends MSELogic
{
    /* Report Constants */
    private final int kRT_HandicapSS_HA         = 1;
    private final int kRT_HeadToHeadSS_HA       = 2;
    
    /* Module Id */
    private final String kModuleId = "106510408";    	
    
	/* Task Ids */ 	 	
    private final String kB_StatsSummary  = "10651040814";

	/* Server Row Names */
   
	/* Client Row Names */    
    private final String kCR_B_Report_Fltr          = "cr11";
    private final String kCR_B_HandicapSS_HA        = "cr12";
    private final String kCR_B_HeadToHeadSS_HA      = "cr13";
    
	/* Status Ids */		

    /* Stats Summary Field */
    private final int f_SSReportTypeId = 0;

    /* Statistics Summary */
   	public Stats01BL ()
	{
		super ();		
	}
	
	/**
        A template method which has been extended from MSELogic.

        @see enlj.component.resource.logics.MSELogic#executeTask(Document oDocument, String oTaskId).
    */   
	public String executeTask (Document oDocument, String oTaskId)
	{		
		String oXMLString = "";
		setParams(oDocument);			

        if (oTaskId.equals (kB_StatsSummary))
        {
            oXMLString = getStatsSummaryData ();
        }
       
		return oXMLString;
	}		
    
    private String getStatsSummaryData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        String [] arrInfo = getParams (kCR_B_Report_Fltr);
        int nReportTypeId = convertToInt (arrInfo [f_SSReportTypeId]);
        
        switch (nReportTypeId)
        {
            case kRT_HandicapSS_HA :
                oBuffer.append (getHandicapSS_HA ());
                break;
            case kRT_HeadToHeadSS_HA :
                oBuffer.append (getHeadToHeadSS_HA ());
                break;                
        }
        
        return oBuffer.toString ();
    } 
    
	private String getHandicapSS_HA ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        int nSportId = SportUtil.kS_BasketBall;
        
        HdpScoreStatsObj oLogic = new HdpScoreStatsObj (getDocument ());
		NodeList arrNodes = getChildNodes (kCR_B_HandicapSS_HA);

		for (int nIndex = 0; nIndex < arrNodes.getLength (); nIndex++)
		{
			Node oNode = arrNodes.item (nIndex);
			String [] arrInfo = getParams (oNode);

	        oBuffer.append (oLogic.getHdpScoreStats (nSportId, arrInfo));
		}

        return oBuffer.toString ();
	}
		
	private String getHeadToHeadSS_HA ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        int nSportId = SportUtil.kS_BasketBall;
        
        HdpScoreStatsObj oLogic = new HdpScoreStatsObj (getDocument ());
		NodeList arrNodes = getChildNodes (kCR_B_HeadToHeadSS_HA);

		for (int nIndex = 0; nIndex < arrNodes.getLength (); nIndex++)
		{
			Node oNode = arrNodes.item (nIndex);
			String [] arrInfo = getParams (oNode);

	        oBuffer.append (oLogic.getHdpScoreStats (nSportId, arrInfo));
		}

        return oBuffer.toString ();
	}
	
	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}