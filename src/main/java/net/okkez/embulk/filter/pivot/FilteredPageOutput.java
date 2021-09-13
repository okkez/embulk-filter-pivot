package net.okkez.embulk.filter.pivot;

import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FilteredPageOutput implements PageOutput {
    private static final Logger log = LoggerFactory.getLogger(FilteredPageOutput.class);
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final List<Column> commonColumns;
    private final List<Column> expandingColumns;

    @SuppressWarnings({"deprecation", "unused"})
    public FilteredPageOutput(PivotFilterPlugin.PluginTask task,
                              Schema inputSchema,
                              Schema outputSchema,
                              PageOutput output,
                              List<Column> commonColumns,
                              List<Column> expandingColumns)
    {
        // Keep compatibility with Embulk-0.9.x
        this.pageReader = new PageReader(inputSchema);
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

        this.commonColumns = commonColumns;
        this.expandingColumns = expandingColumns;
    }

    @Override
    public void add(Page page) {
        pageReader.setPage(page);
        while (pageReader.nextRecord()) {
            try {
                for (Column c: expandingColumns) {
                    int i = 0;
                    for (Column common: commonColumns) {
                        setValue(i, common);
                        i++;
                    }
                    pageBuilder.setString(i, c.getName()); // key
                    i++;
                    setValue(i, c); // value
                    pageBuilder.addRecord();
                }
            } catch(DataException e) {
                log.warn("{}", e.getMessage());
            }
        }
    }

    @SuppressWarnings("deprecation")
    private void setValue(int index, Column c) {
        if (Types.STRING.equals(c.getType())) {
            pageBuilder.setString(index, pageReader.getString(c));
        } else if (Types.DOUBLE.equals(c.getType())) {
            pageBuilder.setDouble(index, pageReader.getDouble(c));
        } else if (Types.LONG.equals(c.getType())) {
            pageBuilder.setLong(index, pageReader.getLong(c));
        } else if (Types.TIMESTAMP.equals(c.getType())) {
            // Compatibility for Embulk-0.9.23
            pageBuilder.setTimestamp(index, pageReader.getTimestamp(c));
        } else if (Types.BOOLEAN.equals(c.getType())) {
            pageBuilder.setBoolean(index, pageReader.getBoolean(c));
        }
    }

    @Override
    public void finish() {
        pageBuilder.finish();
    }

    @Override
    public void close() {
        pageReader.close();
        pageBuilder.close();
    }
}
