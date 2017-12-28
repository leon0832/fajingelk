package com.util;

import com.alibaba.druid.filter.stat.StatFilter;
import com.alibaba.druid.wall.WallFilter;
import com.fajing.model.BasicFile;
import com.jfinal.kit.PropKit;
import com.jfinal.plugin.activerecord.ActiveRecordPlugin;
import com.jfinal.plugin.activerecord.dialect.MysqlDialect;
import com.jfinal.plugin.druid.DruidPlugin;
import org.junit.After;
import org.junit.BeforeClass;

/**
 * @author lh
 */
public class JFinalModelCase {

	private static DruidPlugin dp;

	private static ActiveRecordPlugin activeRecord;

	/**
	 * 数据库类型（如mysql，oracle）
	 */
	private static final String DATABASE_TYPE = "mysql";

	@BeforeClass
	public static void setUpBeforeClass() {
		PropKit.use("init.properties");
		dp = JFinalDemoGenerator.createDruidPlugin();

		dp.addFilter(new StatFilter());

		dp.setInitialSize(3);
		dp.setMinIdle(2);
		dp.setMaxActive(5);
		dp.setMaxWait(60000);
		dp.setTimeBetweenEvictionRunsMillis(120000);
		dp.setMinEvictableIdleTimeMillis(120000);

		WallFilter wall = new WallFilter();
		wall.setDbType(DATABASE_TYPE);
		dp.addFilter(wall);

		dp.getDataSource();
		dp.start();

		activeRecord = new ActiveRecordPlugin(dp);
		activeRecord.setDialect(new MysqlDialect())
				.setDevMode(false)
				//是否打印sql语句
				.setShowSql(true);

		//映射数据库的表和继承与model的实体
		//只有做完该映射后，才能进行junit测试
		activeRecord.addMapping("basic_file", BasicFile.class);

		activeRecord.start();
	}

	@After
	public void tearDown() {
		activeRecord.stop();
		dp.stop();
	}
}
