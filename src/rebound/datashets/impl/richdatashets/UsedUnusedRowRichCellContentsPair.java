package rebound.datashets.impl.richdatashets;

import static java.util.Objects.*;
import javax.annotation.Nonnull;
import rebound.richshets.model.cell.RichshetCellContents;

public class UsedUnusedRowRichCellContentsPair
{
	protected final @Nonnull RichshetCellContents canonicalForUsedRow;
	protected final @Nonnull RichshetCellContents canonicalForUnusedRow;
	
	public UsedUnusedRowRichCellContentsPair(RichshetCellContents canonicalForUsedRow, RichshetCellContents canonicalForUnusedRow)
	{
		this.canonicalForUsedRow = requireNonNull(canonicalForUsedRow);
		this.canonicalForUnusedRow = requireNonNull(canonicalForUnusedRow);
		
		if (canonicalForUsedRow.isEmptyText())
			throw new IllegalArgumentException("The non-empty value of an ignored column can't be empty!");
		
		if (canonicalForUnusedRow.isEmptyText())
			throw new IllegalArgumentException("The empty value of an ignored column can't be non-empty!");
	}
	
	public RichshetCellContents getCanonicalForUsedRow()
	{
		return canonicalForUsedRow;
	}
	
	public RichshetCellContents getCanonicalForUnusedRow()
	{
		return canonicalForUnusedRow;
	}
}
