package enlj.p106trading.mssqlv51.p10651basketball.pbpunterwinlosebydate.logics;

import java.util.*;
import enlj.webenv.utils.*;

public class AmountInfo extends Object
{
	String m_oDateInfo	= "notset";	
	Hashtable m_hashRecordInfo = new Hashtable ();
	Hashtable m_hashRecordWinLose_DR = new Hashtable ();

    public AmountInfo (String oRecordInfo, Hashtable hashGroupInfo)
	{
		m_oDateInfo = oRecordInfo;
		m_hashRecordInfo = hashGroupInfo;
	}

	public void updateRecordColumnInfo (String oRecordId, String oWinLoseInfo)
	{		
		m_hashRecordWinLose_DR.put (oRecordId, oWinLoseInfo);
	}

	public String getRecordInfo (ArrayList arrRecordIds)
	{
		StringBuffer oBuffer = new StringBuffer ("");

		oBuffer.append (m_oDateInfo + DataUtil.kDATA_SEP);
		oBuffer.append (formatRecordInfo_DR (arrRecordIds));

		return oBuffer.toString ();
	}

	protected String formatRecordInfo_DR (ArrayList arrRecordIds)
	{
		StringBuffer oBuffer = new StringBuffer ("");
		
		for (int nIndex = 0; nIndex < arrRecordIds.size (); nIndex++)
		{
			String oRecordId = arrRecordIds.get (nIndex).toString ();
			String oRecordInfo = getRecordInfo (oRecordId);
			String oWinLoseInfo = DataUtil.kEXT_SEP + "0" + DataUtil.kEXT_SEP + "0";
			
			if (m_hashRecordWinLose_DR.containsKey (oRecordId))
				oWinLoseInfo = DataUtil.kEXT_SEP + m_hashRecordWinLose_DR.get (oRecordId).toString ();

			oBuffer.append (oRecordInfo + oWinLoseInfo);
			
			if (nIndex < (arrRecordIds.size () - 1))
				oBuffer.append (DataUtil.kDATA_SEP);
		}
		
		return oBuffer.toString ();
	}
	
	protected String getRecordInfo (String oRecordId)
	{
		String oRecordInfo = "";
		
		if (m_hashRecordInfo.containsKey (oRecordId))
			oRecordInfo = m_hashRecordInfo.get (oRecordId).toString ();
			
		return oRecordInfo;
	}
}