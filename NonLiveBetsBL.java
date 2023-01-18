package enlj.p106trading.mssqlv51.p10651basketball.rbbetsbygroup.logics.nonlivebets;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;

import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.cwagerlist.abetlist.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.aperformance.hpunterperfbybettype.logics.*;

public class NonLiveBetsBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510407";    	
    
	/* Task Ids */ 	 	
    private final String kAC_Report	= "10651040777";
	
	/* Client Row Names */    
    private final String kCR_AC_PunterPerf	= "cr11";
    private final String kCR_AC_BetList		= "cr12";
    
   	public NonLiveBetsBL ()
	{
		super ();		
	}
	
	public String executeTask (Document oDocument, String oTaskId)
	{		
		String oXMLString = "";
		setParams(oDocument);			

        if (oTaskId.equals (kAC_Report))
        {
            oXMLString = getReport ();
        }
       
		return oXMLString;
	}		
    
	private String getReport ()
	{
		StringBuffer oBuffer = new StringBuffer ("");
		
		oBuffer.append (getBetList ());
		oBuffer.append (getPunterPerfByBetType ());
		
		return oBuffer.toString ();
	}
	    
	private String getPunterPerfByBetType ()
    {
		String oXMLString = "";
        String [] arrInfo = getParams (kCR_AC_PunterPerf);
        
        int nReportTypeId = convertToInt (arrInfo [PPBConstants.f_ReportTypeId]);
		PunterPerfByBetTypeObj oPunterPerfObj = new PunterPerfByBetTypeObj (getDocument ());
		
		switch (nReportTypeId)
		{
			case PPBConstants.kRT_Handicap :
				oXMLString = oPunterPerfObj.getPunterPerfByBetType_HDP (SportUtil.kS_BasketBall, arrInfo);
				break;
			case PPBConstants.kRT_OverUnder :
				oXMLString = oPunterPerfObj.getPunterPerfByBetType_OU (SportUtil.kS_BasketBall, arrInfo);
				break;
		}
		
        return oXMLString;
	}

    private String getBetList ()
    {
        String [] arrInfo = getParams (kCR_AC_BetList);
        BetListObj oBetListObj = new BetListObj (getDocument ());
	    return oBetListObj.getBetList (SportUtil.kS_BasketBall, arrInfo);
	}
		
	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}