package rebound.datashets.impl.richdatashets;

import static java.util.Objects.*;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import rebound.richdatashets.api.model.RichdatashetsCellAbsenceStrategy;
import rebound.richshets.model.cell.RichshetsCellContents;

public abstract class ExtendedRichdatashetsSingleValuedCellAbsenceStrategy
{
	protected final @Nonnull RichshetsCellContents canonicalPresentForUsedRow;
	protected final @Nonnull RichshetsCellContents canonicalForUnusedRow;
	
	public ExtendedRichdatashetsSingleValuedCellAbsenceStrategy(RichshetsCellContents canonicalPresentForUsedRow, RichshetsCellContents canonicalForUnusedRow)
	{
		this.canonicalPresentForUsedRow = requireNonNull(canonicalPresentForUsedRow);
		this.canonicalForUnusedRow = requireNonNull(canonicalForUnusedRow);
	}
	
	
	public abstract RichshetsCellContents getCanonicalAbsentForUsedRow();
	
	/**
	 * The text of this will always be overwritten X3
	 */
	public RichshetsCellContents getCanonicalPresentForUsedRow()
	{
		return canonicalPresentForUsedRow;
	}
	
	public RichshetsCellContents getCanonicalForUnusedRow()
	{
		return canonicalForUnusedRow;
	}
	
	
	
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((canonicalForUnusedRow == null) ? 0 : canonicalForUnusedRow.hashCode());
		result = prime * result + ((canonicalPresentForUsedRow == null) ? 0 : canonicalPresentForUsedRow.hashCode());
		return result;
	}
	
	
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExtendedRichdatashetsSingleValuedCellAbsenceStrategy other = (ExtendedRichdatashetsSingleValuedCellAbsenceStrategy) obj;
		if (canonicalForUnusedRow == null)
		{
			if (other.canonicalForUnusedRow != null)
				return false;
		}
		else if (!canonicalForUnusedRow.equals(other.canonicalForUnusedRow))
			return false;
		if (canonicalPresentForUsedRow == null)
		{
			if (other.canonicalPresentForUsedRow != null)
				return false;
		}
		else if (!canonicalPresentForUsedRow.equals(other.canonicalPresentForUsedRow))
			return false;
		return true;
	}
	
	
	
	
	
	
	
	
	
	
	/**
	 * Nulls simply can't be encoded for this column!
	 */
	public static class ExtendedRichdatashetsCellAbsenceStrategyNeverNull
	extends ExtendedRichdatashetsSingleValuedCellAbsenceStrategy
	{
		public ExtendedRichdatashetsCellAbsenceStrategyNeverNull(RichshetsCellContents canonicalPresentForUsedRow, RichshetsCellContents canonicalForUnusedRow)
		{
			super(canonicalPresentForUsedRow, canonicalForUnusedRow);
		}
		
		@Override
		public RichshetsCellContents getCanonicalAbsentForUsedRow()
		{
			throw new UnsupportedOperationException("It should never be needed!");
		}
	}
	
	
	
	
	
	public static class ExtendedRichdatashetsCellAbsenceStrategyNormal
	extends ExtendedRichdatashetsSingleValuedCellAbsenceStrategy
	{
		protected final RichdatashetsCellAbsenceStrategy underlying;
		
		public ExtendedRichdatashetsCellAbsenceStrategyNormal(RichshetsCellContents canonicalPresentForUsedRow, RichshetsCellContents canonicalForUnusedRow, RichdatashetsCellAbsenceStrategy underlying)
		{
			super(canonicalPresentForUsedRow, canonicalForUnusedRow);
			this.underlying = requireNonNull(underlying);
		}
		
		public RichdatashetsCellAbsenceStrategy getUnderlying()
		{
			return underlying;
		}
		
		@Override
		public RichshetsCellContents getCanonicalAbsentForUsedRow()
		{
			return underlying.getAbsentValueForNewCells();
		}
		
		
		
		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + ((underlying == null) ? 0 : underlying.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (getClass() != obj.getClass())
				return false;
			ExtendedRichdatashetsCellAbsenceStrategyNormal other = (ExtendedRichdatashetsCellAbsenceStrategyNormal) obj;
			if (underlying == null)
			{
				if (other.underlying != null)
					return false;
			}
			else if (!underlying.equals(other.underlying))
				return false;
			return true;
		}
	}
	
	
	
	
	
	
	/**
	 * You can, of course, skip all the dealings with richness by just..flat out having a whole other (single-value) column that exists just to keep a boolean of if this column is null or not XD
	 * Note that the "absence strategy" doesn't have to mean that *other* column is absent, just that it indicates this one is!
	 * So it might very well just be a test for if the other columns' cell contains "true" (meaning this one is null) and 
	 */
	public static class ExtendedRichdatashetsCellAbsenceStrategyOtherColumn
	extends ExtendedRichdatashetsSingleValuedCellAbsenceStrategy
	{
		protected final @Nonnull RichshetsCellContents canonicalAbsentForUsedRow;
		
		protected final @Nonnull String uidOfOtherColumn;
		protected final @Nonnull Predicate<RichshetsCellContents> isAbsentBasedOnValueInOtherColumn;
		protected final @Nonnull RichshetsCellContents absentValueInOtherColumnForNewCells;
		protected final @Nonnull RichshetsCellContents presentValueInOtherColumnForNewCells;
		protected final @Nonnull RichshetsCellContents unusedRowValueInOtherColumnForNewCells;
		
		/**
		 * @param uidOfOtherColumn  the uid of the other single-value column that is to be interpreted 
		 * @param isAbsent  Test on the *other* column's cell value as to whether *this* one is absent!
		 * @param absentValueInOtherColumnForNewCells  The canonical value for the other column's contents if this one is null in non-rich Datashets
		 * @param presentValueInOtherColumnForNewCells  The canonical value for the other column's contents if this one is non-null in non-rich Datashets
		 */
		public ExtendedRichdatashetsCellAbsenceStrategyOtherColumn(RichshetsCellContents canonicalPresentForUsedRow, RichshetsCellContents canonicalForUnusedRow, RichshetsCellContents canonicalAbsentForUsedRow,  String uidOfOtherColumn, Predicate<RichshetsCellContents> isAbsent, RichshetsCellContents absentValueInOtherColumnForNewCells, RichshetsCellContents presentValueInOtherColumnForNewCells, RichshetsCellContents unusedRowValueInOtherColumnForNewCells)
		{
			super(canonicalPresentForUsedRow, canonicalForUnusedRow);
			this.canonicalAbsentForUsedRow = requireNonNull(canonicalAbsentForUsedRow);
			this.uidOfOtherColumn = requireNonNull(uidOfOtherColumn);
			this.isAbsentBasedOnValueInOtherColumn = requireNonNull(isAbsent);
			this.absentValueInOtherColumnForNewCells = requireNonNull(absentValueInOtherColumnForNewCells);
			this.presentValueInOtherColumnForNewCells = requireNonNull(presentValueInOtherColumnForNewCells);
			this.unusedRowValueInOtherColumnForNewCells = requireNonNull(unusedRowValueInOtherColumnForNewCells);
		}
		
		@Override
		public RichshetsCellContents getCanonicalAbsentForUsedRow()
		{
			return canonicalAbsentForUsedRow;
		}
		
		
		public String getUIDOfOtherColumn()
		{
			return uidOfOtherColumn;
		}
		
		public boolean isAbsent(RichshetsCellContents otherColumnsCellValue)
		{
			return isAbsentBasedOnValueInOtherColumn.test(otherColumnsCellValue);
		}
		
		public Predicate<RichshetsCellContents> getIsAbsentBasedOnValueInOtherColumn()
		{
			return isAbsentBasedOnValueInOtherColumn;
		}
		
		public RichshetsCellContents getAbsentValueInOtherColumnForNewCells()
		{
			return absentValueInOtherColumnForNewCells;
		}
		
		public RichshetsCellContents getPresentValueInOtherColumnForNewCells()
		{
			return presentValueInOtherColumnForNewCells;
		}
		
		public RichshetsCellContents getUnusedRowValueInOtherColumnForNewCells()
		{
			return unusedRowValueInOtherColumnForNewCells;
		}
		
		
		
		
		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + ((absentValueInOtherColumnForNewCells == null) ? 0 : absentValueInOtherColumnForNewCells.hashCode());
			result = prime * result + ((canonicalAbsentForUsedRow == null) ? 0 : canonicalAbsentForUsedRow.hashCode());
			result = prime * result + ((isAbsentBasedOnValueInOtherColumn == null) ? 0 : isAbsentBasedOnValueInOtherColumn.hashCode());
			result = prime * result + ((presentValueInOtherColumnForNewCells == null) ? 0 : presentValueInOtherColumnForNewCells.hashCode());
			result = prime * result + ((uidOfOtherColumn == null) ? 0 : uidOfOtherColumn.hashCode());
			result = prime * result + ((unusedRowValueInOtherColumnForNewCells == null) ? 0 : unusedRowValueInOtherColumnForNewCells.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (getClass() != obj.getClass())
				return false;
			ExtendedRichdatashetsCellAbsenceStrategyOtherColumn other = (ExtendedRichdatashetsCellAbsenceStrategyOtherColumn) obj;
			if (absentValueInOtherColumnForNewCells == null)
			{
				if (other.absentValueInOtherColumnForNewCells != null)
					return false;
			}
			else if (!absentValueInOtherColumnForNewCells.equals(other.absentValueInOtherColumnForNewCells))
				return false;
			if (canonicalAbsentForUsedRow == null)
			{
				if (other.canonicalAbsentForUsedRow != null)
					return false;
			}
			else if (!canonicalAbsentForUsedRow.equals(other.canonicalAbsentForUsedRow))
				return false;
			if (isAbsentBasedOnValueInOtherColumn == null)
			{
				if (other.isAbsentBasedOnValueInOtherColumn != null)
					return false;
			}
			else if (!isAbsentBasedOnValueInOtherColumn.equals(other.isAbsentBasedOnValueInOtherColumn))
				return false;
			if (presentValueInOtherColumnForNewCells == null)
			{
				if (other.presentValueInOtherColumnForNewCells != null)
					return false;
			}
			else if (!presentValueInOtherColumnForNewCells.equals(other.presentValueInOtherColumnForNewCells))
				return false;
			if (uidOfOtherColumn == null)
			{
				if (other.uidOfOtherColumn != null)
					return false;
			}
			else if (!uidOfOtherColumn.equals(other.uidOfOtherColumn))
				return false;
			if (unusedRowValueInOtherColumnForNewCells == null)
			{
				if (other.unusedRowValueInOtherColumnForNewCells != null)
					return false;
			}
			else if (!unusedRowValueInOtherColumnForNewCells.equals(other.unusedRowValueInOtherColumnForNewCells))
				return false;
			return true;
		}
	}
}
