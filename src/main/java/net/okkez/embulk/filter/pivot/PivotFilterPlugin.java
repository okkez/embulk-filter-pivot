package net.okkez.embulk.filter.pivot;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.ColumnConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PivotFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("common_columns")
        @ConfigDefault("[]")
        List<String> getCommonColumns();

        @Config("key_config")
        @ConfigDefault("{\"name\": \"key\", \"type\": \"string\"}")
        ColumnConfig getKeyConfig();

        @Config("value_config")
        @ConfigDefault("{\"name\": \"value\", \"type\": \"string\"}")
        ColumnConfig getValueConfig();
    }

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    private static final Logger log = LoggerFactory.getLogger(PivotFilterPlugin.class);

    private List<Column> commonColumns;
    private List<Column> expandingColumns;

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
                            FilterPlugin.Control control)
    {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        buildColumns(task, inputSchema);
        Schema outputSchema = buildOutputSchema(task);

        control.run(task.toTaskSource(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
                           Schema outputSchema, PageOutput output)
    {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        return new FilteredPageOutput(task, inputSchema, outputSchema, output, commonColumns, expandingColumns);
    }

    private void buildColumns(PluginTask task, Schema inputSchema)
    {
        this.commonColumns = new ArrayList<>();
        this.expandingColumns = new ArrayList<>();

        for (Column c : inputSchema.getColumns()) {
            if (task.getCommonColumns().contains(c.getName())) {
                this.commonColumns.add(c);
            }
            else {
                this.expandingColumns.add(c);
            }
        }
    }

    private Schema buildOutputSchema(PluginTask task)
    {
        final List<Column> outputColumns = new ArrayList<>();
        int i = 0;
        for (Column c : commonColumns) {
            outputColumns.add(new Column(i, c.getName(), c.getType()));
            i++;
        }

        outputColumns.add(new Column(i, task.getKeyConfig().getName(), task.getKeyConfig().getType()));
        outputColumns.add(new Column(i + 1, task.getValueConfig().getName(), task.getValueConfig().getType()));

        if (log.isDebugEnabled()) {
            for (Column c : outputColumns) {
                log.debug("{}", c);
            }
        }
        return new Schema(outputColumns);
    }
}
