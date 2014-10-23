package com.github.d3rwan.dbtoes;

import org.elasticsearch.client.Client;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
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

import com.github.d3rwan.dbtoes.common.Constants;
import com.github.d3rwan.dbtoes.config.ESConfig;
import com.github.d3rwan.dbtoes.models.Person;
import com.github.d3rwan.dbtoes.processors.PersonItemProcessor;
import com.github.d3rwan.toes.models.ESDocument;
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
	 * Job : Import user from DB into ES
	 * @param jobBuilderFactory factory
	 * @param step1 step to add
	 * @return Job
	 */
	@Bean
	public Job importUserJob(JobBuilderFactory jobBuilderFactory, Step step1) {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.flow(step1)
				.end()
				.build();
	}

	/**
	 * Step 1 : Read from DB, process into ESDocument, index into ES
	 * @param stepBuilderFactory factory
	 * @param reader reader
	 * @param writer writer
	 * @param processor processor
	 * @return Step
	 */
	@Bean
	public Step step1(StepBuilderFactory stepBuilderFactory, ItemReader<Person> reader,
			ItemWriter<ESDocument> writer, ItemProcessor<Person, ESDocument> processor) {
		return stepBuilderFactory.get("step1").<Person, ESDocument> chunk(1000)
				.reader(reader)
				.processor(processor)
				.writer(writer)
				.build();
	}
}