package rebound.datashets.impl.richdatashets;

import static java.util.Collections.*;
import static java.util.Objects.*;
import static rebound.testing.WidespreadTestingUtilities.*;
import static rebound.util.collections.CollectionUtilities.*;
import static rebound.util.objectutil.BasicObjectUtilities.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import rebound.datashets.api.model.DatashetsRow;
import rebound.datashets.api.model.DatashetsSemanticColumns;
import rebound.datashets.api.model.DatashetsTable;
import rebound.datashets.api.model.DatashetsUnusedRow;
import rebound.datashets.api.model.DatashetsUsedRow;
import rebound.datashets.api.operation.DatashetsConnection;
import rebound.datashets.api.operation.DatashetsOperation;
import rebound.datashets.api.operation.DatashetsStructureException;
import rebound.datashets.impl.richdatashets.ExtendedRichdatashetsSingleValuedCellAbsenceStrategy.ExtendedRichdatashetsCellAbsenceStrategyNeverNull;
import rebound.datashets.impl.richdatashets.ExtendedRichdatashetsSingleValuedCellAbsenceStrategy.ExtendedRichdatashetsCellAbsenceStrategyNormal;
import rebound.datashets.impl.richdatashets.ExtendedRichdatashetsSingleValuedCellAbsenceStrategy.ExtendedRichdatashetsCellAbsenceStrategyOtherColumn;
import rebound.richdatashets.api.model.RichdatashetsRow;
import rebound.richdatashets.api.model.RichdatashetsSemanticColumns;
import rebound.richdatashets.api.model.RichdatashetsTable;
import rebound.richdatashets.api.operation.RichdatashetsConnection;
import rebound.richshets.model.cell.RichshetCellContents;
import rebound.util.collections.FilterAwayReturnPath;

public class DericheningDatashetsConnection
implements DatashetsConnection
{
	protected final @Nonnull RichdatashetsConnection underlying;
	protected final @Nonnull DericheningStrategy dericheningStrategy;
	
	public DericheningDatashetsConnection(RichdatashetsConnection underlying, DericheningStrategy strategy)
	{
		this.underlying = requireNonNull(underlying);
		this.dericheningStrategy = requireNonNull(strategy);
	}
	
	public RichdatashetsConnection getUnderlying()
	{
		return underlying;
	}
	
	public DericheningStrategy getDericheningStrategy()
	{
		return dericheningStrategy;
	}
	
	
	
	
	
	
	
	@Override
	public void perform(boolean performMaintenance, DatashetsOperation operation) throws DatashetsStructureException, IOException
	{
		underlying.perform(performMaintenance, operation == null ? null : inputRich ->
		{
			validateForRichColumns(inputRich.getColumnsSingleValued().getUIDs(), inputRich.getColumnsMultiValued().getUIDs());
			
			DatashetsTable inputPlain = convertFromRich(inputRich);
			
			boolean[] unusedRowsInOriginalRich;
			{
				int n = inputRich.getNumberOfRows();
				
				if (inputPlain.getNumberOfRows() != n)
					throw new AssertionError();
				
				unusedRowsInOriginalRich = new boolean[n];
				
				for (int i = 0; i < n; i++)
					unusedRowsInOriginalRich[i] = inputPlain.getRows().get(i) instanceof DatashetsUnusedRow;
			}
			
			if (inputPlain != null)  //Todo disable someday when we feel safe this class' code is right XD'
				validateForPlainColumns(inputPlain.getColumnsSingleValued().getUIDs(), inputPlain.getColumnsMultiValued().getUIDs());
			
			DatashetsTable outputPlain = operation.performInMemory(inputPlain);
			
			if (outputPlain != null)
				validateForPlainColumns(outputPlain.getColumnsSingleValued().getUIDs(), outputPlain.getColumnsMultiValued().getUIDs());
			
			RichdatashetsTable outputRich = outputPlain == null ? null : convertToRich(outputPlain, inputRich, unusedRowsInOriginalRich);
			
			if (outputRich != null)  //Todo disable someday when we feel safe this class' code is right XD'
				validateForRichColumns(outputRich.getColumnsSingleValued().getUIDs(), outputRich.getColumnsMultiValued().getUIDs());
			
			return outputRich;
		});
	}
	
	
	
	public DatashetsTable convertFromRich(RichdatashetsTable rich)
	{
		RichdatashetsSemanticColumns columnsSingleValuedRich = rich.getColumnsSingleValued();
		RichdatashetsSemanticColumns columnsMultiValuedRich = rich.getColumnsMultiValued();
		
		int nColumnsSingleValuedRich = columnsSingleValuedRich.size();
		
		DatashetsSemanticColumns columnsSingleValuedPlain;
		Set<String> columnsSingleValuedIgnored;
		{
			columnsSingleValuedIgnored = new HashSet<>(columnsSingleValuedRich.size());
			
			columnsSingleValuedIgnored.addAll(dericheningStrategy.getSingleValueColumnsToIgnore().keySet());
			
			for (Entry<String, ExtendedRichdatashetsSingleValuedCellAbsenceStrategy> e : dericheningStrategy.getSingleValueColumnAbsenceStrategies().entrySet())
			{
				ExtendedRichdatashetsSingleValuedCellAbsenceStrategy a = e.getValue();
				
				if (a instanceof ExtendedRichdatashetsCellAbsenceStrategyOtherColumn)
				{
					String uidOther = ((ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) a).getUIDOfOtherColumn();
					
					if (!dericheningStrategy.getSingleValueColumnAbsenceStrategies().containsKey(uidOther))
						columnsSingleValuedIgnored.add(uidOther);
				}
			}
			
			columnsSingleValuedPlain = convertColumnsFromRich(columnsSingleValuedRich, columnsSingleValuedIgnored);
		}
		
		
		
		List<RichdatashetsRow> rowsRich = rich.getRows();
		
		List<DatashetsRow> rowsPlain;
		{
			rowsPlain = new ArrayList<>(rowsRich.size());
			
			int nMulti = columnsMultiValuedRich.size();
			
			
			//Pre-calculate Map.get() for all these for speed when there's like..a *million rows* XD''
			int[] ignoredSinglesIndexes;  //indexes here are arbitrary and meaningless X3
			{
				int n = columnsSingleValuedIgnored.size();
				
				ignoredSinglesIndexes = new int[n];
				
				int i = 0;
				for (String uid : columnsSingleValuedIgnored)
				{
					ignoredSinglesIndexes[i] = columnsSingleValuedRich.requireIndexByUID(uid);
					i++;
				}
			}
			
			
			//Pre-calculate Map.get() for all these for speed when there's like..a *million rows* XD''
			//The indexes here are the Single-Valued Column Indexes in the Datashets form!
			int nUnignoredSingles;
			int[] unignoredSinglesIndexes;  //the values here are Single-Valued Column Indexes in the Richdatashets form
			ExtendedRichdatashetsSingleValuedCellAbsenceStrategy[] unignoredSinglesAbsenceStrategies;
			int[] unignoredSinglesAbsencesStrategiesOtherColumnIndexes;  //this is just a cache of the column index lookup-by-uid of ExtendedRichdatashetsCellAbsenceStrategyOtherColumn.getUIDOfOtherColumn()  (or -1 if the absence strategy isn't that since it won't be used)
			{
				nUnignoredSingles = columnsSingleValuedPlain.size();
				
				unignoredSinglesIndexes = new int[nUnignoredSingles];
				unignoredSinglesAbsenceStrategies = new ExtendedRichdatashetsSingleValuedCellAbsenceStrategy[nUnignoredSingles];
				unignoredSinglesAbsencesStrategiesOtherColumnIndexes = new int[nUnignoredSingles];
				
				for (int columnIndexPlain = 0; columnIndexPlain < nUnignoredSingles; columnIndexPlain++)
				{
					String uid = columnsSingleValuedPlain.getUIDByIndex(columnIndexPlain);
					
					int columnIndexRich = columnsSingleValuedRich.requireIndexByUID(uid);
					ExtendedRichdatashetsSingleValuedCellAbsenceStrategy absenceStrategy = dericheningStrategy.getSingleValueColumnAbsenceStrategies().get(uid);
					
					if (absenceStrategy == null)
						throw new IllegalStateException("The "+DericheningStrategy.class.getSimpleName()+" doesn't have an entry in the single-value column absence strategies for the column with UID = \""+uid+"\"");
					
					int absencesStrategyOtherColumnIndex = absenceStrategy instanceof ExtendedRichdatashetsCellAbsenceStrategyOtherColumn ? columnsSingleValuedRich.requireIndexByUID(((ExtendedRichdatashetsCellAbsenceStrategyOtherColumn)absenceStrategy).getUIDOfOtherColumn()) : -1;
					
					
					unignoredSinglesIndexes[columnIndexPlain] = columnIndexRich;
					unignoredSinglesAbsenceStrategies[columnIndexPlain] = absenceStrategy;
					unignoredSinglesAbsencesStrategiesOtherColumnIndexes[columnIndexPlain] = absencesStrategyOtherColumnIndex;
				}
			}
			
			
			int nRows = rich.getNumberOfRows();
			for (int rowIndexRich = 0; rowIndexRich < nRows; rowIndexRich++)
			{
				RichdatashetsRow rowRich = rowsRich.get(rowIndexRich);
				
				boolean isUnusedRow;
				{
					isUnusedRow = true;
					
					for (int i = 0; i < nMulti; i++)
					{
						if (!rowRich.getMultiValuedColumns().get(i).isEmpty())
						{
							isUnusedRow = false;
							break;
						}
					}
					
					if (!isUnusedRow)
					{
						//						for (int ignoredSingleValueColumnIndex : ignoredSinglesIndexes)
						//						{
						//							RichdatashetsCellContents v = rowRich.getSingleValuedColumns().get(ignoredSingleValueColumnIndex);
						//							
						//							if (!v.isEmptyText())
						//							{
						//								isUnusedRow = false;
						//								break;
						//							}
						//						}
						//						
						//						if (!isUnusedRow)
						//						{
						//							for (int i = 0; i < nUnignoredSingles; i++)
						//							{
						//								int columnIndexRich = unignoredSinglesIndexes[i];
						//								ExtendedRichdatashetsCellAbsenceStrategy absenceStrategy = unignoredSinglesAbsenceStrategies[i];
						//								int absencesStrategyOtherColumnIndex = unignoredSinglesAbsencesStrategiesOtherColumnIndexes[i];
						//								
						//								RichdatashetsCellContents cell = rowRich.getSingleValuedColumns().get(columnIndexRich);
						//								
						//								boolean isUnused = isSingleValuedCellNull(cell, absenceStrategy, x -> rowRich.getSingleValuedColumns().get(absencesStrategyOtherColumnIndex));
						//								
						//								if (!isUnused)
						//								{
						//									isUnusedRow = false;
						//									break;
						//								}
						//							}
						//						}
						
						
						//Actually let's just make this simpler (at least for now) XD''
						//a row is Unused if all multi-valued cells are empty and all single-valued cells have an empty string!
						//Hence the need for a "*" column if that can be legitimate and Used!
						{
							for (int columnIndexRich = 0; columnIndexRich < nColumnsSingleValuedRich; columnIndexRich++)
							{
								RichshetCellContents v = rowRich.getSingleValuedColumns().get(columnIndexRich);
								
								if (!v.isEmptyText())
								{
									isUnusedRow = false;
									break;
								}
							}
						}
					}
				}
				
				
				
				if (isUnusedRow)
				{
					rowsPlain.add(new DatashetsUnusedRow(rowIndexRich));
				}
				else
				{
					DatashetsRow rowPlain;
					{
						List<String> singleValueCellContentsPlain;
						{
							singleValueCellContentsPlain = new ArrayList<>();
							
							for (int i = 0; i < nUnignoredSingles; i++)
							{
								int columnIndexRich = unignoredSinglesIndexes[i];
								ExtendedRichdatashetsSingleValuedCellAbsenceStrategy absenceStrategy = unignoredSinglesAbsenceStrategies[i];
								int absencesStrategyOtherColumnIndex = unignoredSinglesAbsencesStrategiesOtherColumnIndexes[i];
								
								RichshetCellContents cell = rowRich.getSingleValuedColumns().get(columnIndexRich);
								
								boolean isAbsentCell = isSingleValuedCellNull(cell, absenceStrategy, x -> rowRich.getSingleValuedColumns().get(absencesStrategyOtherColumnIndex));
								
								String plainValue = isAbsentCell ? null : cell.justText();
								
								singleValueCellContentsPlain.add(plainValue);
							}
						}
						
						List<List<String>> multiValueCellContentsPlain;
						{
							multiValueCellContentsPlain = new ArrayList<>(nMulti);
							
							for (int i = 0; i < nMulti; i++)
							{
								List<RichshetCellContents> r = rowRich.getMultiValuedColumns().get(i);
								
								int n = r.size();
								List<String> p = new ArrayList<>(n);
								
								for (int j = 0; j < n; j++)
									p.add(r.get(j).justText());
							}
						}
						
						rowPlain = new DatashetsUsedRow(singleValueCellContentsPlain, multiValueCellContentsPlain, rowIndexRich);  //original indexes here point to richdatashets' indexes!  and those point to the more underlying thing (eg, richsheets / the whole spreadsheet file!)
					}
					
					rowsPlain.add(rowPlain);
				}
			}
		}
		
		
		
		DatashetsSemanticColumns columnsMultiValuedPlain = convertColumnsFromRich(columnsMultiValuedRich, emptySet());
		
		return new DatashetsTable(columnsSingleValuedPlain, columnsMultiValuedPlain, rowsPlain);
	}
	
	
	@FunctionalInterface
	protected static interface UnaryFunction<I, O> { public O f(I a); }
	
	protected boolean isSingleValuedCellNull(RichshetCellContents cell, ExtendedRichdatashetsSingleValuedCellAbsenceStrategy absenceStrategy, UnaryFunction<String, RichshetCellContents> getOtherColumn)
	{
		if (absenceStrategy instanceof ExtendedRichdatashetsCellAbsenceStrategyNeverNull)
		{
			return false;
		}
		else if (absenceStrategy instanceof ExtendedRichdatashetsCellAbsenceStrategyNormal)
		{
			return ((ExtendedRichdatashetsCellAbsenceStrategyNormal) absenceStrategy).getUnderlying().isAbsent(cell);
		}
		else if (absenceStrategy instanceof ExtendedRichdatashetsCellAbsenceStrategyOtherColumn)
		{
			ExtendedRichdatashetsCellAbsenceStrategyOtherColumn cas = (ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) absenceStrategy;
			RichshetCellContents otherCell = getOtherColumn.f(cas.getUIDOfOtherColumn());
			return cas.isAbsent(otherCell);
		}
		else
		{
			throw new AssertionError();
		}
	}
	
	
	
	protected RichdatashetsTable convertToRich(DatashetsTable plain, @Nonnull RichdatashetsTable originalRich, boolean[] unusedRowsInOriginalRich)
	{
		RichdatashetsSemanticColumns columnsSingleValuedRich = originalRich.getColumnsSingleValued();
		RichdatashetsSemanticColumns columnsMultiValuedRich = originalRich.getColumnsMultiValued();
		
		DatashetsSemanticColumns columnsSingleValuedPlain = plain.getColumnsSingleValued();
		DatashetsSemanticColumns columnsMultiValuedPlain = plain.getColumnsMultiValued();
		
		
		List<RichdatashetsRow> rowsRich;
		{
			List<DatashetsRow> rowsPlain = plain.getRows();
			
			rowsRich = new ArrayList<>(rowsPlain.size());
			
			int nMulti = columnsMultiValuedRich.size();
			RichshetCellContents[] richMultiColumnTemplates;  //indexes are rich-form indexes
			int[] multiColumnIndexesInPlain;  //indexes are rich-form indexes and values are plain-form indexes
			{
				richMultiColumnTemplates = new RichshetCellContents[nMulti];
				multiColumnIndexesInPlain = new int[nMulti];
				
				for (int i = 0; i < nMulti; i++)
				{
					String uid = columnsMultiValuedRich.getUIDByIndex(i);
					
					RichshetCellContents template = dericheningStrategy.getMultiValueColumnFormattingTemplates().get(uid);
					
					if (template == null)
						throw new AssertionError(uid);  //This should have been checked for already!
					
					richMultiColumnTemplates[i] = template;
					multiColumnIndexesInPlain[i] = columnsMultiValuedPlain.requireIndexByUID(uid);
				}
			}
			
			
			//Indexes here match to underlying rich-form single-value column indexes (which we made sure to match between the input and output, just to make our lives easier in this one function, not because we needed to!)
			int nSingleRich = columnsSingleValuedRich.size();
			boolean[] richSingleColumnsIsIsnullAndUsesOtherColumnDataInConfig;
			Object[] richSingleColumnsConfigs;  //UsedUnusedRowRichCellContentsPair for explicit-ignoreds, ExtendedRichdatashetsCellAbsenceStrategyOtherColumn for all others (its own for normals, the defining one for "IsNull?"s)
			Object[] richSingleColumnsPlains;  //null for ignored ones, int (Integer) column index for normals, for "IsNull?" ones this is a int[n>1] (or Integer if n=1) of the nullable plain Datashets column indexes this implicit-ignored one marks as null or not!
			{
				richSingleColumnsIsIsnullAndUsesOtherColumnDataInConfig = new boolean[nSingleRich];
				richSingleColumnsConfigs = new Object[nSingleRich];
				richSingleColumnsPlains = new Object[nSingleRich];
				
				Map<String, String> usersToIsnullCols = maptodictSameKeys(uid ->
				{
					ExtendedRichdatashetsSingleValuedCellAbsenceStrategy a = dericheningStrategy.getSingleValueColumnAbsenceStrategies().get(uid);
					
					if (a instanceof ExtendedRichdatashetsCellAbsenceStrategyOtherColumn)
						return ((ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) a).getUIDOfOtherColumn();
					else
						throw FilterAwayReturnPath.I;
					
				}, columnsSingleValuedRich.getUIDs());
				
				
				Map<String, Set<String>> isnullColsToUsers = inverseMapGeneralOP(usersToIsnullCols);
				
				
				
				for (int richColumnIndex = 0; richColumnIndex < nSingleRich; richColumnIndex++)
				{
					String uid = columnsSingleValuedRich.getUIDByIndex(richColumnIndex);
					
					Set<String> users = isnullColsToUsers.get(uid);
					
					if (users != null)
						asrt(!users.isEmpty());
					
					UsedUnusedRowRichCellContentsPair explicitIgnoredStuff = dericheningStrategy.getSingleValueColumnsToIgnore().get(uid);
					
					ExtendedRichdatashetsSingleValuedCellAbsenceStrategy ownAbsenceStrategy = dericheningStrategy.getSingleValueColumnAbsenceStrategies().get(uid);
					
					if (explicitIgnoredStuff != null)
					{
						asrt(ownAbsenceStrategy == null);  //this should have been checked for in new DericheningStrategy(...) or validateForXyzColumns(...) 
						asrt(users == null);  //this should have been checked for in new DericheningStrategy(...) or validateForXyzColumns(...) 
					}
					else if (ownAbsenceStrategy != null)
					{
						//asrt(explicitIgnoredStuff == null);
						asrt(users == null);  //this should have been checked for in new DericheningStrategy(...) or validateForXyzColumns(...) 
					}
					else
					{
						//asrt(explicitIgnoredStuff == null);
						asrt(users != null);  //this should have been checked for in new DericheningStrategy(...) or validateForXyzColumns(...) 
					}
					
					boolean isnullColumn = ownAbsenceStrategy == null && users != null;
					
					
					
					richSingleColumnsIsIsnullAndUsesOtherColumnDataInConfig[richColumnIndex] = isnullColumn;
					
					if (explicitIgnoredStuff != null)
					{
						richSingleColumnsConfigs[richColumnIndex] = explicitIgnoredStuff;
						richSingleColumnsPlains[richColumnIndex] = null;
					}
					else if (isnullColumn)
					{
						ExtendedRichdatashetsCellAbsenceStrategyOtherColumn definer;
						int[] definingColumnsPlainIndexes;
						{
							definer = null;
							definingColumnsPlainIndexes = new int[users.size()];
							
							int i = 0;
							
							for (String definingColumnUID : users)
							{
								ExtendedRichdatashetsSingleValuedCellAbsenceStrategy userAS = getMandatory(dericheningStrategy.getSingleValueColumnAbsenceStrategies(), definingColumnUID);
								ExtendedRichdatashetsCellAbsenceStrategyOtherColumn userOCAS = (ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) userAS;
								
								if (definer == null)
									definer = userOCAS;
								else
									asrt(eq(definer, userOCAS));  //this should have been checked for in new DericheningStrategy(...) 
								
								definingColumnsPlainIndexes[i] = columnsSingleValuedPlain.requireIndexByUID(definingColumnUID);
								i++;
							}
						}
						
						richSingleColumnsConfigs[richColumnIndex] = definer;
						richSingleColumnsPlains[richColumnIndex] = definingColumnsPlainIndexes;
					}
					else
					{
						richSingleColumnsConfigs[richColumnIndex] = ownAbsenceStrategy;
						richSingleColumnsPlains[richColumnIndex] = columnsSingleValuedPlain.requireIndexByUID(uid);
					}
				}
			}
			
			
			
			for (DatashetsRow rowPlain : rowsPlain)
			{
				RichdatashetsRow rowRich;
				{
					int originalIndexInRich = rowPlain.getOriginalRowIndex();
					
					if (originalIndexInRich < -1)
						throw new IllegalStateException();
					
					if (originalIndexInRich >= originalRich.getNumberOfRows())
						throw new IndexOutOfBoundsException();
					
					RichdatashetsRow originalRow = originalIndexInRich == -1 ? null : requireNonNull(originalRich.getRows().get(originalIndexInRich));
					
					
					//New one is an unused row!
					if (rowPlain instanceof DatashetsUnusedRow)
					{
						if (originalIndexInRich == -1 || !unusedRowsInOriginalRich[originalIndexInRich])
						{
							//If it's a new row, or the original row was Used not Unused,
							//Make it from scratch! :3
							
							List<RichshetCellContents> s;
							{
								s = new ArrayList<>(nSingleRich);
								
								for (int columnIndexRich = 0; columnIndexRich < nSingleRich; columnIndexRich++)
								{
									RichshetCellContents richCellValueForUnusedRow;
									{
										Object x = richSingleColumnsConfigs[columnIndexRich];
										
										if (richSingleColumnsIsIsnullAndUsesOtherColumnDataInConfig[columnIndexRich])
										{
											//Implicitly-Ignored Single-Valued Column (eg, a "Is The One Right Of Me Null?" column!)
											ExtendedRichdatashetsCellAbsenceStrategyOtherColumn c = (ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) x;
											richCellValueForUnusedRow = c.getUnusedRowValueInOtherColumnForNewCells();
										}
										else if (x instanceof ExtendedRichdatashetsSingleValuedCellAbsenceStrategy)
										{
											//Normal Single-Valued Column
											ExtendedRichdatashetsSingleValuedCellAbsenceStrategy c = (ExtendedRichdatashetsSingleValuedCellAbsenceStrategy) x;
											richCellValueForUnusedRow = c.getCanonicalForUnusedRow();
										}
										else
										{
											//Explicitly-Ignored Single-Valued Column (eg, the "*" column!)
											UsedUnusedRowRichCellContentsPair c = (UsedUnusedRowRichCellContentsPair) x;
											if (richSingleColumnsIsIsnullAndUsesOtherColumnDataInConfig[columnIndexRich])
												throw new AssertionError();
											richCellValueForUnusedRow = c.getCanonicalForUnusedRow();
										}
									}
									
									s.add(richCellValueForUnusedRow);
								}
							}
							
							List<List<RichshetCellContents>> m;
							{
								m = new ArrayList<>(nMulti);
								for (int i = 0; i < nMulti; i++)
									m.add(new ArrayList<>());
							}
							
							rowRich = new RichdatashetsRow(s, m, originalRow == null ? -1 : originalRow.getOriginalDataRowIndex());
						}
						else
						{
							//Otherwise it was unused before and it's still unused, so just copy it verbatim! :>
							
							List<List<RichshetCellContents>> m;
							{
								List<List<RichshetCellContents>> l = originalRow.getMultiValuedColumns();
								m = new ArrayList<>(l.size());
								for (List<RichshetCellContents> ll : l)
									m.add(new ArrayList<>(ll));
							}
							
							rowRich = new RichdatashetsRow(new ArrayList<>(originalRow.getSingleValuedColumns()), m, originalRow.getOriginalDataRowIndex());
						}
					}
					
					
					//New one is a used row!
					else
					{
						DatashetsUsedRow rowPlainUsed = (DatashetsUsedRow) rowPlain;
						
						List<RichshetCellContents> s;
						{
							s = new ArrayList<>(nSingleRich);
							
							for (int columnIndexRich = 0; columnIndexRich < nSingleRich; columnIndexRich++)  //remember, we explicitly (arbitrarily) made sure old-rich and new-rich form column indexes match up :3    (normally that's not always the case!)
							{
								//Wait we can't copy the formatting!  The formatting is what distinguishes null/absent from notnull/present in many cases!! XD'''
								//								@Nullable RichdatashetsCellContents template;
								//								{
								//									if (originalIndexInRich == -1 || unusedRowsInOriginalRich[originalIndexInRich] || canonicalizeFormatting)
								//									{
								//										//If it's a new row, or the original row was Unused not Used,
								//										//Make it from scratch! :3
								//										template = null;
								//									}
								//									else
								//									{
								//										//Otherwise it was unused before and it's still unused, so copy its format if possible! :>
								//										template = originalRow.getSingleValuedColumns().get(columnIndexRich);
								//									}
								//								}
								
								
								RichshetCellContents richCellValue;
								{
									Object x = richSingleColumnsConfigs[columnIndexRich];
									
									if (richSingleColumnsIsIsnullAndUsesOtherColumnDataInConfig[columnIndexRich])
									{
										//Implicitly-Ignored Single-Valued Column (eg, a "Is The One Right Of Me Null?" column!)
										ExtendedRichdatashetsCellAbsenceStrategyOtherColumn c = (ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) x;
										
										Object p = richSingleColumnsPlains[columnIndexRich];
										
										boolean isnull;
										
										if (p instanceof int[])
										{
											int[] plains = (int[]) p;
											
											boolean allNull = true;
											boolean anyNull = false;
											{
												for (int plainIndex : plains)
												{
													boolean thisnull = rowPlainUsed.getSingleValuedColumns().get(plainIndex) == null;
													allNull &= thisnull;
													anyNull |= thisnull;
												}
											}
											
											if (allNull != anyNull)
												throw new DatashetsStructureException("If multiple columns share the same IsNull? pseudocolumn, then they have to all be null together or all be nonnull together; otherwise the next time the rich-form data is read into plain-form, they *will* be one or the other!!");
											
											isnull = allNull;  // = anyNull
										}
										else
										{
											int plainIndex = (Integer)p;
											isnull = rowPlainUsed.getSingleValuedColumns().get(plainIndex) == null;
										}
										
										RichshetCellContents valueForUsTheOtherColumn = isnull ? c.getAbsentValueInOtherColumnForNewCells() : c.getPresentValueInOtherColumnForNewCells();
										
										richCellValue = valueForUsTheOtherColumn;
									}
									else if (x instanceof ExtendedRichdatashetsSingleValuedCellAbsenceStrategy)
									{
										//Normal Single-Valued Column
										ExtendedRichdatashetsSingleValuedCellAbsenceStrategy c = (ExtendedRichdatashetsSingleValuedCellAbsenceStrategy) x;
										
										Object p = richSingleColumnsPlains[columnIndexRich];
										int plainIndex = (Integer)p;
										
										@Nullable String plainCellValue = rowPlainUsed.getSingleValuedColumns().get(plainIndex);
										
										richCellValue = plainCellValue == null ? c.getCanonicalAbsentForUsedRow() : c.getCanonicalPresentForUsedRow().withOtherText(plainCellValue);
									}
									else
									{
										//Explicitly-Ignored Single-Valued Column (eg, the "*" column!)
										UsedUnusedRowRichCellContentsPair c = (UsedUnusedRowRichCellContentsPair) x;
										if (richSingleColumnsIsIsnullAndUsesOtherColumnDataInConfig[columnIndexRich])
											throw new AssertionError();
										richCellValue = c.getCanonicalForUsedRow();
									}
								}
								
								s.add(richCellValue);
							}
						}
						
						List<List<RichshetCellContents>> m;
						{
							m = new ArrayList<>(nMulti);
							
							for (int columnIndexRich = 0; columnIndexRich < nMulti; columnIndexRich++)  //remember, we explicitly (arbitrarily) made sure old-rich and new-rich form column indexes match up :3    (normally that's not always the case!)
							{
								int columnIndexPlain = multiColumnIndexesInPlain[columnIndexRich];
								List<String> lPlain = rowPlainUsed.getMultiValuedColumns().get(columnIndexPlain);
								
								RichshetCellContents template = richMultiColumnTemplates[columnIndexRich];
								
								List<RichshetCellContents> lRich = new ArrayList<>(lPlain.size());
								
								for (String ePlain : lPlain)
								{
									requireNonNull(ePlain);  //multi-value strings are nonnull in both plain and rich Datashets.
									RichshetCellContents eRich = template.withOtherText(ePlain);
									lRich.add(eRich);
								}
								
								m.add(lRich);
							}
						}
						
						rowRich = new RichdatashetsRow(s, m, originalRow == null ? -1 : originalRow.getOriginalDataRowIndex());
					}
				}
				
				rowsRich.add(rowRich);
			}
		}
		
		
		return new RichdatashetsTable(columnsSingleValuedRich, columnsMultiValuedRich, rowsRich);
	}
	
	
	//Todo public RichdatashetsTable convertToNewRich(DatashetsTable plain)
	
	
	
	
	
	
	public DatashetsSemanticColumns convertColumnsFromRich(RichdatashetsSemanticColumns rich, Set<String> singleValuesToIgnore)
	{
		return new DatashetsSemanticColumns(filterToList(c -> !singleValuesToIgnore.contains(c), rich.getUIDs()));
	}
	
	//Todo public RichdatashetsSemanticColumns convertColumnsToRich(DatashetsSemanticColumns plain, Set<String> singleValuesToInclude)
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	protected void validateForRichColumns(Collection<String> singleValueUIDs, Collection<String> multiValueUIDs) throws DatashetsStructureException
	{
		validateSameSetwise(multiValueUIDs, dericheningStrategy.getMultiValueColumnFormattingTemplates().keySet(), "Multivalued Columns");
		
		Set<String> allSingles;
		{
			allSingles = new HashSet<>(dericheningStrategy.getSingleValueColumnAbsenceStrategies().keySet());
			allSingles.addAll(dericheningStrategy.getSingleValueColumnsToIgnore().keySet());
			
			for (ExtendedRichdatashetsSingleValuedCellAbsenceStrategy v : dericheningStrategy.getSingleValueColumnAbsenceStrategies().values())
			{
				if (v instanceof ExtendedRichdatashetsCellAbsenceStrategyOtherColumn)
					allSingles.add(((ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) v).getUIDOfOtherColumn());  //this add() must be lenient since it might already be in there!
			}
		}
		
		validateSameSetwise(singleValueUIDs, allSingles, "Singlevalued Columns");
	}
	
	protected void validateForPlainColumns(Collection<String> singleValueUIDs, Collection<String> multiValueUIDs) throws DatashetsStructureException
	{
		validateSameSetwise(multiValueUIDs, dericheningStrategy.getMultiValueColumnFormattingTemplates().keySet(), "Multivalued Columns");
		validateSameSetwise(singleValueUIDs, dericheningStrategy.getSingleValueColumnAbsenceStrategies().keySet(), "Singlevalued Columns");
	}
	
	
	protected static void validateSameSetwise(Collection<String> outside, Collection<String> internal, String errorMessageContext) throws DatashetsStructureException
	{
		if (!setsEqv(outside, internal))
		{
			Set<String> outsideExtras = setdiff(outside, internal);
			Set<String> internalExtras = setdiff(internal, outside);
			
			throw new DatashetsStructureException(errorMessageContext+" did not match the "+DericheningStrategy.class.getSimpleName()+"!  These extras were present only in the given table: "+outsideExtras+"    And these were missing: "+internalExtras);
		}
	}
	
	protected static <E> boolean setsEqv(Collection<E> a, Collection<E> b)
	{
		if (a.size() != b.size())
			return false;
		
		//No need for {for (E e : b) a.contains(e) etc.c.} because of the pidgeonhole principle :3
		for (E e : a)
			if (!b.contains(e))
				return false;
		return true;
	}
	
	protected static <E> Set<E> setdiff(Collection<E> minuend, Collection<E> subtrahend)
	{
		Set<E> s = new HashSet<>(minuend);
		s.removeAll(subtrahend);
		return s;
	}
}
