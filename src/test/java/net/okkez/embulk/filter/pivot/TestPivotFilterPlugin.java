package net.okkez.embulk.filter.pivot;

import net.okkez.embulk.filter.pivot.PivotFilterPlugin.PluginTask;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Types;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPivotFilterPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private PivotFilterPlugin plugin;

    @Before
    public void createResource()
    {
        plugin = new PivotFilterPlugin();
    }

    private ConfigSource defaultConfig()
    {
        return runtime.getExec().newConfigSource()
                .set("type", "pivot");
    }

    private interface CallbackWithPage
    {
        void run(PageReader pageReader, MockPageOutput pageOutput);
    }

    private void applyFilter(ConfigSource config, final Schema inputSchema, final List<Object> rawRecord, CallbackWithPage callback)
    {
        plugin.transaction(config, inputSchema, (taskSource, outputSchema) -> {
            final MockPageOutput filteredOutput = new MockPageOutput();
            PageOutput pageOutput = plugin.open(taskSource, inputSchema, outputSchema, filteredOutput);
            List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, rawRecord.toArray());
            for (Page page : pages) {
                pageOutput.add(page);
            }
            pageOutput.finish();
            pageOutput.close();

            // We must keep compatibility with embulk-0.9.x
            PageReader pageReader = new PageReader(outputSchema);
            callback.run(pageReader, filteredOutput);
        });
    }

    @Test
    public void testConfigDefaultValue()
    {
        ConfigSource config = defaultConfig();
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        List<String> commonColumns = task.getCommonColumns();
        assertTrue(commonColumns.isEmpty());
    }

    @Test
    public void testTransactionWithDefaultConfig()
    {
        ConfigSource config = defaultConfig();
        final Schema inputSchema = Schema.builder()
                .add("user_id", Types.STRING)
                .add("gender", Types.STRING)
                .add("city", Types.STRING)
                .build();

        plugin.transaction(config, inputSchema, ((taskSource, outputSchema) -> {
            assertEquals(2, outputSchema.getColumnCount());
            assertEquals("key", outputSchema.getColumns().get(0).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(0).getType());
            assertEquals("value", outputSchema.getColumns().get(1).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(1).getType());
        }));
    }

    @Test
    public void testTransactionWithOneCommonColumn()
    {
        ConfigSource config = defaultConfig()
                .set("common_columns", Arrays.asList("user_id"));
        final Schema inputSchema = Schema.builder()
                .add("user_id", Types.STRING)
                .add("gender", Types.STRING)
                .add("city", Types.STRING)
                .build();

        plugin.transaction(config, inputSchema, ((taskSource, outputSchema) -> {
            assertEquals(3, outputSchema.getColumnCount());
            assertEquals("user_id", outputSchema.getColumns().get(0).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(0).getType());
            assertEquals("key", outputSchema.getColumns().get(1).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(1).getType());
            assertEquals("value", outputSchema.getColumns().get(2).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(2).getType());
        }));
    }

    @Test
    public void testTransactionWithOneCommonColumnAndCustomKeyValueName()
    {
        ConfigSource keyConfig = runtime.getExec().newConfigSource()
                .set("name", "custom_key")
                .set("type", "string");
        ConfigSource valueConfig = runtime.getExec().newConfigSource()
                .set("name", "custom_value")
                .set("type", "string");
        ConfigSource config = defaultConfig()
                .set("common_columns", Arrays.asList("user_id"))
                .setNested("key_config", keyConfig)
                .setNested("value_config", valueConfig);
        final Schema inputSchema = Schema.builder()
                .add("user_id", Types.STRING)
                .add("gender", Types.STRING)
                .add("city", Types.STRING)
                .build();

        plugin.transaction(config, inputSchema, ((taskSource, outputSchema) -> {
            assertEquals(3, outputSchema.getColumnCount());
            assertEquals("user_id", outputSchema.getColumns().get(0).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(0).getType());
            assertEquals("custom_key", outputSchema.getColumns().get(1).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(1).getType());
            assertEquals("custom_value", outputSchema.getColumns().get(2).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(2).getType());
        }));
    }

    @Test
    public void testTransactionWithTwoCommonColumns()
    {
        ConfigSource config = defaultConfig()
                .set("common_columns", Arrays.asList("user_id", "project"));
        final Schema inputSchema = Schema.builder()
                .add("user_id", Types.STRING)
                .add("project", Types.STRING)
                .add("gender", Types.STRING)
                .add("city", Types.STRING)
                .build();

        plugin.transaction(config, inputSchema, ((taskSource, outputSchema) -> {
            assertEquals(4, outputSchema.getColumnCount());
            assertEquals("user_id", outputSchema.getColumns().get(0).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(0).getType());
            assertEquals("project", outputSchema.getColumns().get(1).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(1).getType());
            assertEquals("key", outputSchema.getColumns().get(2).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(2).getType());
            assertEquals("value", outputSchema.getColumns().get(3).getName());
            assertEquals(Types.STRING, outputSchema.getColumns().get(3).getType());
        }));
    }

    @Test
    public void testOneRecord()
    {
        ConfigSource config = defaultConfig();
        Schema inputSchema = Schema.builder()
                .add("user_id", Types.STRING)
                .build();

        applyFilter(config, inputSchema, Arrays.asList("user-123"), (pageReader, pageOutput) -> {
            assertEquals(1, pageOutput.pages.size());
            pageReader.setPage(pageOutput.pages.get(0));
            assertTrue(pageReader.nextRecord());
            assertEquals("user_id", pageReader.getString(0));
            assertEquals("user-123", pageReader.getString(1));
        });
    }

    @Test
    public void testOneRecordWithOneCommonColumn()
    {
        ConfigSource config = defaultConfig()
                .set("common_columns", Arrays.asList("user_id"));
        final Schema inputSchema = Schema.builder()
                .add("user_id", Types.STRING)
                .add("gender", Types.STRING)
                .add("city", Types.STRING)
                .build();

        applyFilter(config, inputSchema, Arrays.asList("user-123", "male", "Tokyo"), (pageReader, pageOutput) -> {
            assertEquals(1, pageOutput.pages.size());
            for (Page page : pageOutput.pages) {
                pageReader.setPage(page);
            }

            assertTrue(pageReader.nextRecord());
            assertEquals("user-123", pageReader.getString(0));
            assertEquals("gender", pageReader.getString(1));
            assertEquals("male", pageReader.getString(2));
            assertTrue(pageReader.nextRecord());
            assertEquals("user-123", pageReader.getString(0));
            assertEquals("city", pageReader.getString(1));
            assertEquals("Tokyo", pageReader.getString(2));
        });
    }

    @Test
    public void testOneRecordWithTwoCommonColumn()
    {
        ConfigSource config = defaultConfig()
                .set("common_columns", Arrays.asList("user_id", "project"));
        final Schema inputSchema = Schema.builder()
                .add("user_id", Types.STRING)
                .add("project", Types.STRING)
                .add("gender", Types.STRING)
                .add("city", Types.STRING)
                .build();

        applyFilter(config, inputSchema, Arrays.asList("user-123", "project-x", "male", "Tokyo"), (pageReader, pageOutput) -> {
            assertEquals(1, pageOutput.pages.size());
            for (Page page : pageOutput.pages) {
                pageReader.setPage(page);
            }

            assertTrue(pageReader.nextRecord());
            assertEquals("user-123", pageReader.getString(0));
            assertEquals("project-x", pageReader.getString(1));
            assertEquals("gender", pageReader.getString(2));
            assertEquals("male", pageReader.getString(3));
            assertTrue(pageReader.nextRecord());
            assertEquals("user-123", pageReader.getString(0));
            assertEquals("project-x", pageReader.getString(1));
            assertEquals("city", pageReader.getString(2));
            assertEquals("Tokyo", pageReader.getString(3));
        });
    }

    @Test
    public void testOneRecordWithOneCommonColumnAndCustomKeyValueName()
    {
        ConfigSource keyConfig = runtime.getExec().newConfigSource()
                .set("name", "custom_key")
                .set("type", "string");
        ConfigSource valueConfig = runtime.getExec().newConfigSource()
                .set("name", "custom_value")
                .set("type", "string");
        ConfigSource config = defaultConfig()
                .set("common_columns", Arrays.asList("user_id"))
                .setNested("key_config", keyConfig)
                .setNested("value_config", valueConfig);
        final Schema inputSchema = Schema.builder()
                .add("user_id", Types.STRING)
                .add("gender", Types.STRING)
                .add("city", Types.STRING)
                .build();

        applyFilter(config, inputSchema, Arrays.asList("user-123", "male", "Tokyo"), (pageReader, pageOutput) -> {
            assertEquals(1, pageOutput.pages.size());
            for (Page page : pageOutput.pages) {
                pageReader.setPage(page);
            }

            assertTrue(pageReader.nextRecord());
            assertEquals("user-123", pageReader.getString(0));
            assertEquals("gender", pageReader.getString(1));
            assertEquals("male", pageReader.getString(2));
            assertTrue(pageReader.nextRecord());
            assertEquals("user-123", pageReader.getString(0));
            assertEquals("city", pageReader.getString(1));
            assertEquals("Tokyo", pageReader.getString(2));
        });
    }

    @Test
    public void testOneRecordWithCustomType()
    {
        ConfigSource keyConfig = runtime.getExec().newConfigSource()
                .set("name", "custom_key")
                .set("type", "string");
        ConfigSource valueConfig = runtime.getExec().newConfigSource()
                .set("name", "custom_value")
                .set("type", "long");
        ConfigSource config = defaultConfig()
                .set("common_columns", Arrays.asList("user_id"))
                .setNested("key_config", keyConfig)
                .setNested("value_config", valueConfig);
        final Schema inputSchema = Schema.builder()
                .add("user_id", Types.LONG)
                .add("age", Types.LONG)
                .add("score", Types.LONG)
                .build();

        applyFilter(config, inputSchema, Arrays.asList(123L, 20L, 999L), (pageReader, pageOutput) -> {
            assertEquals(1, pageOutput.pages.size());
            for (Page page : pageOutput.pages) {
                pageReader.setPage(page);
            }

            assertTrue(pageReader.nextRecord());
            assertEquals(123L, pageReader.getLong(0));
            assertEquals("age", pageReader.getString(1));
            assertEquals(20L, pageReader.getLong(2));
            assertTrue(pageReader.nextRecord());
            assertEquals(123L, pageReader.getLong(0));
            assertEquals("score", pageReader.getString(1));
            assertEquals(999L, pageReader.getLong(2));
        });
    }
}
