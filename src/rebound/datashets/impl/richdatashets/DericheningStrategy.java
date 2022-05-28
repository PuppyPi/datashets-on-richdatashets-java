package rebound.datashets.impl.richdatashets;

import static java.util.Objects.*;
import static rebound.util.collections.CollectionUtilities.*;
import static rebound.util.objectutil.BasicObjectUtilities.*;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nonnull;
import rebound.datashets.impl.richdatashets.ExtendedRichdatashetsSingleValuedCellAbsenceStrategy.ExtendedRichdatashetsCellAbsenceStrategyOtherColumn;
import rebound.richdatashets.api.model.RichdatashetsCellAbsenceStrategy;
import rebound.richshets.model.cell.RichshetCellContents;
import rebound.util.collections.FilterAwayReturnPath;

public class DericheningStrategy
{
	protected final @Nonnull Map<String, UsedUnusedRowRichCellContentsPair> singleValueColumnsToIgnore;
	protected final @Nonnull Map<String, ExtendedRichdatashetsSingleValuedCellAbsenceStrategy> singleValueColumnAbsenceStrategies;
	protected final @Nonnull Map<String, RichshetCellContents> multiValueColumnFormattingTemplates;
	
	
	
	public DericheningStrategy(@Nonnull Map<String, UsedUnusedRowRichCellContentsPair> singleValueColumnsToIgnore, @Nonnull Map<String, ExtendedRichdatashetsSingleValuedCellAbsenceStrategy> singleValueColumnAbsenceStrategies, @Nonnull Map<String, RichshetCellContents> multiValueColumnFormattingTemplates)
	{
		requireNonNull(singleValueColumnsToIgnore);
		requireNonNull(singleValueColumnAbsenceStrategies);
		requireNonNull(multiValueColumnFormattingTemplates);
		
		
		//Check multis
		for (Entry<String, RichshetCellContents> e : multiValueColumnFormattingTemplates.entrySet())
		{
			requireNonNull(e.getKey());
			requireNonNull(e.getValue());
		}
		
		
		
		//Check singles to ignore
		for (Entry<String, UsedUnusedRowRichCellContentsPair> e : singleValueColumnsToIgnore.entrySet())
		{
			requireNonNull(e.getKey());
			requireNonNull(e.getValue());
			
			if (multiValueColumnFormattingTemplates.containsKey(e.getKey()))
				throw new IllegalArgumentException("Single-Valued and Multi-Valued columns can't have the same UID! XD");
		}
		
		
		
		//Check singles to not ignore
		for (Entry<String, ExtendedRichdatashetsSingleValuedCellAbsenceStrategy> e : singleValueColumnAbsenceStrategies.entrySet())
		{
			requireNonNull(e.getKey());
			requireNonNull(e.getValue());
			
			String uid = e.getKey();
			ExtendedRichdatashetsSingleValuedCellAbsenceStrategy as = e.getValue();
			
			if (multiValueColumnFormattingTemplates.containsKey(uid))
				throw new IllegalArgumentException("Single-Valued and Multi-Valued columns can't have the same UID! XD");
			
			if (singleValueColumnsToIgnore.containsKey(uid))
				throw new IllegalArgumentException("Unignored columns can't be in the ignored set!");
			
			
			if (as instanceof ExtendedRichdatashetsCellAbsenceStrategyOtherColumn)
			{
				String uidOther = ((ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) as).getUIDOfOtherColumn();
				
				if (multiValueColumnFormattingTemplates.containsKey(uidOther))
					throw new IllegalArgumentException("IsNull? columns can't be in the multivalue columns set!");
				
				if (singleValueColumnsToIgnore.containsKey(uidOther))
					throw new IllegalArgumentException("IsNull? columns can't be in the ignored set!");
				
				if (singleValueColumnAbsenceStrategies.containsKey(uidOther))
					throw new IllegalArgumentException("IsNull? columns can't be in the normal-columns set!");
			}
		}
		
		
		
		
		//Check inter-column relationships!
		{
			Map<String, String> usersToIsnullCols = maptodictSameKeys(uid ->
			{
				ExtendedRichdatashetsSingleValuedCellAbsenceStrategy a = singleValueColumnAbsenceStrategies.get(uid);
				
				if (a instanceof ExtendedRichdatashetsCellAbsenceStrategyOtherColumn)
					return ((ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) a).getUIDOfOtherColumn();
				else
					throw FilterAwayReturnPath.I;
				
			}, singleValueColumnAbsenceStrategies.keySet());
			
			
			Map<String, Set<String>> isnullColsToUsers = inverseMapGeneralOP(usersToIsnullCols);
			
			
			for (Entry<String, Set<String>> e : isnullColsToUsers.entrySet())
			{
				String uid = e.getKey();
				Set<String> users = e.getValue();
				
				ExtendedRichdatashetsCellAbsenceStrategyOtherColumn definer = null;
				
				for (String definingColumnUID : users)
				{
					ExtendedRichdatashetsSingleValuedCellAbsenceStrategy userAS = getMandatory(singleValueColumnAbsenceStrategies, definingColumnUID);
					ExtendedRichdatashetsCellAbsenceStrategyOtherColumn userOCAS = (ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) userAS;
					
					if (definer == null)
						definer = userOCAS;
					else if (!eq(definer, userOCAS))
						throw new IllegalArgumentException("The IsNull? column "+uid+" was defined by differing "+ExtendedRichdatashetsCellAbsenceStrategyOtherColumn.class.getSimpleName()+"'s!  They must all be the same if they're for the same IsNull? column that applies to multiple other columns!  (ideally, Java Reference-Identical, but that's not required)");
				}
			}
		}
		
		
		
		this.singleValueColumnsToIgnore = singleValueColumnsToIgnore;
		this.singleValueColumnAbsenceStrategies = singleValueColumnAbsenceStrategies;
		this.multiValueColumnFormattingTemplates = multiValueColumnFormattingTemplates;
	}
	
	
	
	
	/**
	 * Sometimes all cells can legitimately be empty/absent but the row isn't meant to be a Datashets-Absent Row.
	 * In that case, the convention is to make a column with the UID "*" that contains "*" for every non-absent row just so it won't be totally empty.
	 * You set this to {@link Collections#singleton(Object)}("*") in that case.
	 * Otherwise set this to {@link Collections#emptySet()}.
	 * 
	 * But it can be any list of single-value or multi-value column UIDs!
	 * 
	 * We consider the cell to disqualify the row from being blank solely by {@link RichdatashetsCellAbsenceStrategy#EmptyTextCellStrategy} reasoning (just the text being empty).  All multivalues are considered blank for blank rows if they're empty, and all single values according to their entry in {@link #getSingleValueColumnAbsenceStrategies()}.
	 * When we go to write back out the table, the values here are used for new or changed rows.
	 */
	public Map<String, UsedUnusedRowRichCellContentsPair> getSingleValueColumnsToIgnore()
	{
		return singleValueColumnsToIgnore;
	}
	
	/**
	 * In Datashets unlike Richdatashets, individual single-value cells can be empty!
	 * (That's one of the major differences between the two!)
	 * 
	 * This is how that's determined. :3
	 * 
	 * Note that multiple columns can use the same {@link ExtendedRichdatashetsCellAbsenceStrategyOtherColumn#getUIDOfOtherColumn() "Null?" other-column}, and other-columns can also be a key here!  (but if they're not, then they won't appear in the (non-rich) Datashets form!)
	 * They can't be in {@link #getSingleValueColumnsToIgnore()} though (and neither can any of the keys here be)
	 * 
	 * + Also note that this is a superset of the input columns (like with multivalue columns in the conversion from richsheets → richdatashets), so it's okay if some given here aren't in the input, but it's not okay if any single-value columns in the input table aren't listed here as either a key or {@link ExtendedRichdatashetsCellAbsenceStrategyOtherColumn#getUIDOfOtherColumn() other-column} value (or {@link #getSingleValueColumnsToIgnore()})
	 */
	public Map<String, ExtendedRichdatashetsSingleValuedCellAbsenceStrategy> getSingleValueColumnAbsenceStrategies()
	{
		return singleValueColumnAbsenceStrategies;
	}
	
	/**
	 * The text of the values here will always be overwritten X3
	 */
	public Map<String, RichshetCellContents> getMultiValueColumnFormattingTemplates()
	{
		return multiValueColumnFormattingTemplates;
	}
}
