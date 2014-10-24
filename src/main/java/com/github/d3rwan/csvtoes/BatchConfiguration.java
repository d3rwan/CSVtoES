package com.github.d3rwan.csvtoes;

import org.elasticsearch.client.Client;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;

import com.github.d3rwan.csvtoes.common.Constants;
import com.github.d3rwan.csvtoes.config.ESConfig;
import com.github.d3rwan.csvtoes.models.Person;
import com.github.d3rwan.csvtoes.processors.PersonItemProcessor;
import com.github.d3rwan.toes.models.ESDocument;
import com.github.d3rwan.toes.tasklets.AddAliasESTasklet;
import com.github.d3rwan.toes.tasklets.CreateIndexESTasklet;
import com.github.d3rwan.toes.tasklets.DeleteIndexESTasklet;
import com.github.d3rwan.toes.tasklets.PutMappingESTasklet;
import com.github.d3rwan.toes.writers.ESItemWriter;

@Configuration
@Import({ESConfig.class})
@EnableBatchProcessing
public class BatchConfiguration {

	/** environment */
	@Autowired
	private Environment environment;

	/** ES client */
	@Autowired
	private Client esClient;

	/** Job builder factory */
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	/** Step builder factory */
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	/**
	 * Job : Import user from DB into ES
	 * @param jobBuilderFactory factory
	 * @param step1 step to add
	 * @return Job
	 */
	@Bean
	public Job indexPersonJob() {
		return jobBuilderFactory.get("indexPersonJob")
				.start(step1())
				.next(step2())
				.next(step3())
				.next(step4())
				.next(step5())
				.next(step6())
				.build();
	}

	/**
	 * Step 1 : Delete index if already exist
	 * @return Step
	 */
	@Bean
	public Step step1() {
		DeleteIndexESTasklet deleteIndexIfAlreadyExist = new DeleteIndexESTasklet();
		deleteIndexIfAlreadyExist.setIndex(environment.getProperty(Constants.CONFIG_ES_INDEX));
		deleteIndexIfAlreadyExist.setEsClient(esClient);
	    return stepBuilderFactory.get("step1")
	    		.tasklet(deleteIndexIfAlreadyExist)
	    		.build();
	}

	/**
	 * Step 2 : Create index
	 * @return Step
	 */
	@Bean
	public Step step2() {
		CreateIndexESTasklet createIndex = new CreateIndexESTasklet();
		createIndex.setIndex(environment.getProperty(Constants.CONFIG_ES_INDEX));
		createIndex.setEsClient(esClient);
		createIndex.setSettings(new ClassPathResource(
				environment.getProperty(Constants.CONFIG_ES_SETTINGS_FILENAME)));
	    return stepBuilderFactory.get("step2")
	    		.tasklet(createIndex)
	    		.build();
	}

	/**
	 * Step 3 : Put mapping
	 * @return Step
	 */
	@Bean
	public Step step3() {
		PutMappingESTasklet putMapping = new PutMappingESTasklet();
		putMapping.setIndex(environment.getProperty(Constants.CONFIG_ES_INDEX));
		putMapping.setType(environment.getProperty(Constants.CONFIG_ES_TYPE));
		putMapping.setEsClient(esClient);
		putMapping.setMapping(new ClassPathResource(
				environment.getProperty(Constants.CONFIG_ES_MAPPING_FILENAME)));
	    return stepBuilderFactory.get("step3")
	    		.tasklet(putMapping)
	    		.build();
	}

	/**
	 * Step 4 : Read from DB, process into ESDocument, index into ES
	 * @return Step
	 */
	@Bean
	public Step step4() {
		return stepBuilderFactory.get("step4").<Person, ESDocument> chunk(1000)
				.reader(reader())
				.processor(processor())
				.writer(writer())
				.build();
	}

	/**
	 * Read into CSV
	 * @return HibernateCursorItemReader
	 */
	@Bean
    public ItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource(environment.getProperty(Constants.CONFIG_CSV_PATH)));
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer(",") {{
                setNames(new String[] { "id", "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

	/**
	 * Process person into ESDocument
	 * @return ItemProcessor
	 */
	@Bean
	public ItemProcessor<Person, ESDocument> processor() {
		return new PersonItemProcessor();
	}

	/**
	 * Writer for ES
	 * @param esClient ES client
	 * @return ItemWriter
	 */
	@Bean
	public ItemWriter<ESDocument> writer() {
		ESItemWriter<ESDocument> writer = new ESItemWriter<ESDocument>(
				esClient, environment.getProperty(Constants.CONFIG_ES_TIMEOUT));
		return writer;
	}

	/**
	 * Step 5 : Delete old index
	 * @return Step
	 */
	@Bean
	public Step step5() {
		DeleteIndexESTasklet deleteOldIndex = new DeleteIndexESTasklet();
		deleteOldIndex.setIndex(environment.getProperty(Constants.CONFIG_ES_ALIAS));
		deleteOldIndex.setEsClient(esClient);
	    return stepBuilderFactory.get("step5")
	    		.tasklet(deleteOldIndex)
	    		.build();
	}

	/**
	 * Step 6 : Add alias
	 * @return Step
	 */
	@Bean
	public Step step6() {
		AddAliasESTasklet addAlias = new AddAliasESTasklet();
		addAlias.setIndex(environment.getProperty(Constants.CONFIG_ES_INDEX));
		addAlias.setAlias(environment.getProperty(Constants.CONFIG_ES_ALIAS));
		addAlias.setEsClient(esClient);
	    return stepBuilderFactory.get("step6")
	    		.tasklet(addAlias)
	    		.build();
	}
}