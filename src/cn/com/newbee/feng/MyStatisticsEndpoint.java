package cn.com.newbee.feng;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.example.RowCountEndpoint;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import cn.com.newbee.feng.Statistics.protointerface.MyStatisticsInterface;
import cn.com.newbee.feng.Statistics.protointerface.MyStatisticsInterface.getStatisticsRequest;
import cn.com.newbee.feng.Statistics.protointerface.MyStatisticsInterface.getStatisticsResponse;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * 统计行数和统计指定列值加和的Endpoint实现
 * 
 * @author newbee-feng
 *
 */
public class MyStatisticsEndpoint extends
		MyStatisticsInterface.myStatisticsService implements Coprocessor,
		CoprocessorService {
	private static final Log LOG = LogFactory.getLog(MyStatisticsEndpoint.class);

	private static final String STATISTICS_COUNT = "COUNT";

	private static final String STATISTICS_SUM = "SUM";

	// 单个region的上下文环境信息
	private RegionCoprocessorEnvironment envi;

	// rpc服务，返回本身即可，因为此类实例就是一个服务实现
	@Override
	public Service getService() {
		return this;
	}

	// 协处理器是运行于region中的，每一个region都会加载协处理器
	// 这个方法会在regionserver打开region时候执行（还没有真正打开）
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		LOG.info("MyStatisticsEndpoint start");
		// 需要检查当前环境是否在region上
		if (env instanceof RegionCoprocessorEnvironment) {
			this.envi = (RegionCoprocessorEnvironment) env;

		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}

	}

	// 这个方法会在regionserver关闭region时候执行（还没有真正关闭）
	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		LOG.info("MyStatisticsEndpoint stop");
		// nothing to do

	}

	// 服务端（每一个region上）的接口实现方法
	// 第一个参数是固定的，其余的request参数和response参数是proto接口文件中指明的。
	@Override
	public void getStatisticsResult(RpcController controller,
			getStatisticsRequest request,
			RpcCallback<getStatisticsResponse> done) {
		LOG.info("MyStatisticsEndpoint##getStatisticsResult call");
		// 单个region上的计算结果值
		int result = 0;

		// 定义返回response
		MyStatisticsInterface.getStatisticsResponse.Builder responseBuilder = MyStatisticsInterface.getStatisticsResponse
				.newBuilder();

		// type就是在proto中定义参数字段，如果有多个参数字段可以都可以使用request.getXxx()来获取
		String type = request.getType();

		// 查看当前需要做和计算
		if (null == type) {
			LOG.error("the type is null");
			responseBuilder.setResult(result);
			done.run(responseBuilder.build());
			return;
		}

		// 当进行行数统计的时
		if (STATISTICS_COUNT.equals(type)) {
			LOG.info("MyStatisticsEndpoint##getStatisticsResult call " + STATISTICS_COUNT);
			InternalScanner scanner = null;
			try {
				Scan scan = new Scan();
				scanner = this.envi.getRegion().getScanner(scan);
				List<Cell> results = new ArrayList<Cell>();
				boolean hasMore = false;

				do {
					hasMore = scanner.next(results);
					result++;
				} while (hasMore);
			} catch (IOException ioe) {
				LOG.error("error happend when count in "
						+ this.envi.getRegion().getRegionNameAsString()
						+ " error is " + ioe);
			} finally {
				if (scanner != null) {
					try {
						scanner.close();
					} catch (IOException ignored) {
						// nothing to do 
					}
				}
			}
			
		}
		// 当进行指定列值统计的时候
		else if (STATISTICS_SUM.equals(type)) {
			LOG.info("MyStatisticsEndpoint##getStatisticsResult call " + STATISTICS_SUM);
			// 此时需要去检查客户端是否指定了列族和列名
			String famillyName = request.getFamillyName();
			String columnName = request.getColumnName();

			// 此条件下列族和列名是必须的
			if (!StringUtils.isBlank(famillyName)
					&& !StringUtils.isBlank(columnName)) {
				
				InternalScanner scanner = null;
				try {
					Scan scan = new Scan();
					scan.addColumn(Bytes.toBytes(famillyName), Bytes.toBytes(columnName));
					scanner = this.envi.getRegion().getScanner(scan);
					List<Cell> results = new ArrayList<Cell>();
					boolean hasMore = false;
					do {
						hasMore = scanner.next(results);
						if (results.size() == 1)
						{
							// 按行读取数据，并进行加和操作
							result = result + Integer.valueOf(Bytes.toString(CellUtil.cloneValue(results.get(0))));
						}
						
						results.clear();
					} while (hasMore);
					
				} catch (Exception e) {
					LOG.error("error happend when count in "
							+ this.envi.getRegion().getRegionNameAsString()
							+ " error is " + e);
				} finally {
					if (scanner != null) {
						try {
							scanner.close();
						} catch (IOException ignored) {
							// nothing to do 
						}
					}
				}
				

			} else {
				LOG.error("did not specify the famillyName or columnName!");
			}

		}
		// 如果不在此范围内则不计算
		else {
			LOG.error("the type is not match!");
		}
		
		LOG.info("MyStatisticsEndpoint##getStatisticsResult call end");

		responseBuilder.setResult(result);
		done.run(responseBuilder.build());
		return;

	}

}
