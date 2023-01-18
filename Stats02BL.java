package enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.statistics.stats02;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.anormaltable.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.ghandicapwld.logics.*;

public class Stats02BL extends MSELogic
{
    /* Report Constants */
    private final int kRT_HandicapWLD_HA    = 1;
    private final int kRT_HandicapWLD_Home  = 2;
    private final int kRT_HandicapWLD_Away  = 3;
    private final int kRT_LeagueTable       = 4;

    /* Module Id */
    private final String kModuleId = "106510408";    	
    
	/* Task Ids */ 	 	
    private final String kC_HandicapStats  = "10651040817";

	/* Server Row Names */
   
	/* Client Row Names */    
    private final String kCR_C_Report_Fltr      = "cr11";
    private final String kCR_C_HandicapWLD_HA   = "cr12";
    private final String kCR_C_HandicapWLD_Home = "cr13";
    private final String kCR_C_HandicapWLD_Away = "cr14";
    private final String kCR_C_LeagueTable      = "cr15";
    
	/* Status Ids */		

    /* Handicap Performance Chart Field */
    private final int f_HPReportTypeId = 0;


    /* Handicap Performance Chart */
   	public Stats02BL ()
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

        if (oTaskId.equals (kC_HandicapStats))
        {
            oXMLString = getHandicapStatsData ();
        }
       
		return oXMLString;
	}		
    
    private String getHandicapStatsData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        String [] arrInfo = getParams (kCR_C_Report_Fltr);
        int nReportTypeId = convertToInt (arrInfo [f_HPReportTypeId]);
        
        switch (nReportTypeId)
        {
            case kRT_HandicapWLD_HA :
                oBuffer.append (getHandicapWLD_HA ());
                break;
            case kRT_HandicapWLD_Home :
                oBuffer.append (getHandicapWLD_Home ());
                break;
            case kRT_HandicapWLD_Away :
                oBuffer.append (getHandicapWLD_Away ());
                break;
            case kRT_LeagueTable :
                oBuffer.append (getLeagueTable ());
                break;
        }
        
        return oBuffer.toString ();
    } 
    
    private String getHandicapWLD_HA ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        int nSportId = SportUtil.kS_BasketBall;
                    
		String [] arrInfo = getParams (kCR_C_HandicapWLD_HA);				
		HandicapWLDObj oLogic = new HandicapWLDObj (getDocument ());

		oBuffer.append (oLogic.getHandicapWLD (nSportId, arrInfo));		

        return oBuffer.toString ();
	}

    private String getHandicapWLD_Home ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        int nSportId = SportUtil.kS_BasketBall;
                    
		String [] arrInfo = getParams (kCR_C_HandicapWLD_Home);				
		HandicapWLDObj oLogic = new HandicapWLDObj (getDocument ());

		oBuffer.append (oLogic.getHandicapWLD (nSportId, arrInfo));		

        return oBuffer.toString ();
	}
	
	private String getHandicapWLD_Away ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        int nSportId = SportUtil.kS_BasketBall;
                    
		String [] arrInfo = getParams (kCR_C_HandicapWLD_Away);				
		HandicapWLDObj oLogic = new HandicapWLDObj (getDocument ());

		oBuffer.append (oLogic.getHandicapWLD (nSportId, arrInfo));		

        return oBuffer.toString ();
	}
	
    private String getLeagueTable ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        
        int nSportId = SportUtil.kS_BasketBall;
                    
		String [] arrInfo = getParams (kCR_C_LeagueTable);				
		LeagueTableObj oLogic = new LeagueTableObj (getDocument ());
		oBuffer.append (oLogic.getLeagueTable (nSportId, arrInfo));		

        return oBuffer.toString ();
	}    
	
	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}