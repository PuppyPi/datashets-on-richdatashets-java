package rebound.datashets.impl.richdatashets;

import static java.util.Objects.*;
import javax.annotation.Nonnull;
import rebound.richshets.model.cell.RichshetsCellContents;

public class UsedUnusedRowRichCellContentsPair
{
	protected final @Nonnull RichshetsCellContents canonicalForUsedRow;
	protected final @Nonnull RichshetsCellContents canonicalForUnusedRow;
	
	public UsedUnusedRowRichCellContentsPair(RichshetsCellContents canonicalForUsedRow, RichshetsCellContents canonicalForUnusedRow)
	{
		this.canonicalForUsedRow = requireNonNull(canonicalForUsedRow);
		this.canonicalForUnusedRow = requireNonNull(canonicalForUnusedRow);
		
		if (canonicalForUsedRow.isEmptyText())
			throw new IllegalArgumentException("The non-empty value of an ignored column can't be empty!");
		
		if (canonicalForUnusedRow.isEmptyText())
			throw new IllegalArgumentException("The empty value of an ignored column can't be non-empty!");
	}
	
	public RichshetsCellContents getCanonicalForUsedRow()
	{
		return canonicalForUsedRow;
	}
	
	public RichshetsCellContents getCanonicalForUnusedRow()
	{
		return canonicalForUnusedRow;
	}
}
