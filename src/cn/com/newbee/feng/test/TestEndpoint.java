package cn.com.newbee.feng.test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import cn.com.newbee.feng.Statistics.protointerface.MyStatisticsInterface;
import cn.com.newbee.feng.Statistics.protointerface.MyStatisticsInterface.getStatisticsResponse;
import cn.com.newbee.feng.Statistics.protointerface.MyStatisticsInterface.myStatisticsService;

/**
 * 测试endpoint
 * 
 * @author newbeefeng
 *
 */
public class TestEndpoint {

	public static void main(String[] args) throws Throwable {
		// 在hbase客户端进程中保持一个Configuration即可
		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "xufeng-1,xufeng-2,xufeng-3");

		// // 通过CoprocessorRpcChannel coprocessorService(byte[] row)
		// // 请求单个region的rpc服务
		// // 计算指定rowkey所在的region上的数据行数
		 System.out.println("singleRegionStatistics COUNT = "
		 + singleRegionStatistics(config, "coprocessor_table", "row2",
		 "COUNT", null, null));
		 // 计算指定rowkey所在的region上的F列族下的A列的值得求和
		 System.out.println("singleRegionStatistics SUM = "
		 + singleRegionStatistics(config, "coprocessor_table", "row2",
		 "SUM", "F", "A"));

		// 通过Table.coprocessorService(Class, byte[], byte[],Batch.Call)
		// 请求多个region的rpc服务
		// 计算指定rowkey范围所在的region上的数据行数
		 System.out.println("multipleRegionsStatistics COUNT = "
		 + multipleRegionsStatistics(config, "coprocessor_table",
		 "row1", "row3", "COUNT", null, null));
		 // 计算指定rowkey范围所在的region上的F列族下的A列的值得求和
		 System.out.println("multipleRegionsStatistics SUM = "
		 + multipleRegionsStatistics(config, "coprocessor_table",
		 "row1", "row3", "SUM", "F", "A"));

		// 通过Table.coprocessorService(Class, byte[],
		// byte[],Batch.Call,Batch.Callback)
		// 请求多个region的rpc服务
		// 计算指定rowkey范围所在的region上的数据行数
		System.out.println("multipleRegionsStatistics COUNT = "
				+ multipleRegionsCallBackStatistics(config,
						"coprocessor_table", "row1", "row3", "COUNT", null,
						null));
		// 计算指定rowkey范围所在的region上的F列族下的A列的值得求和
		System.out.println("multipleRegionsStatistics SUM = "
				+ multipleRegionsCallBackStatistics(config,
						"coprocessor_table", "row1", "row3", "SUM", "F", "A"));

	}

	/**
	 * 通过CoprocessorRpcChannel coprocessorService(byte[] row); 请求单region服务
	 * 
	 * 客户端通过rowKey的指定，指向rowKey所在的region进行服务请求,所以从数据上来说只有这个region所包含的数据范围
	 * 另外由于只向单个region请求服务，所以在客户端也没有必要在做归并操作。
	 * 
	 * @param config
	 * @param tableName
	 * @param rowkey
	 * @param type
	 * @param famillyName
	 * @param columnName
	 * @return
	 * @throws IOException
	 */
	private static long singleRegionStatistics(Configuration config,
			String tableName, String rowkey, String type, String famillyName,
			String columnName) throws IOException {
		long result = 0;
		Table table = null;
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));

			// 每一个region都加载了Endpoint协处理器，换句话说每一个region都能提供rpc的service服务，首先确定调用的范围
			// 这里只通过一个rowkey来确定，不管在此表中此rowkey是否存在，只要某个region的范围包含了这个rowkey，则这个region就为客户端提供服务
			CoprocessorRpcChannel channel = table.coprocessorService(rowkey
					.getBytes());

			// 因为在region上可能会有很多不同rpcservice，所以必须确定你需要哪一个service
			MyStatisticsInterface.myStatisticsService.BlockingInterface service = MyStatisticsInterface.myStatisticsService
					.newBlockingStub(channel);

			// 构建参数，设置 RPC 入口参数
			MyStatisticsInterface.getStatisticsRequest.Builder request = MyStatisticsInterface.getStatisticsRequest
					.newBuilder();
			request.setType(type);
			if (null != famillyName) {
				request.setFamillyName(famillyName);
			}

			if (null != columnName) {
				request.setColumnName(columnName);
			}

			// 调用 RPC
			MyStatisticsInterface.getStatisticsResponse ret = service
					.getStatisticsResult(null, request.build());

			// 解析结果,由于只向一个region请求服务，所以在客户端也就不存在去归并的操作
			result = ret.getResult();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != table) {
				table.close();
			}

			if (null != connection) {
				connection.close();
			}
		}
		return result;
	}

	/**
	 * 通过Table.coprocessorService(Class, byte[], byte[],Batch.Call) 请求多region服务
	 * 
	 * 在这个API参数列表中， 　　　*　Class代表所需要请求的服务，当前是我们定义在proto中的myStatisticsService服务。
	 * 　　　*　byte[],
	 * byte[]参数指明了startRowKey和endRowKey，当都为null的时候即进行全表的全region的数据计算。
	 * 　　　*　Batch.
	 * Call：需要自定义，API会根据如上参数信息并行的连接各个region，来执行这个参数中定义的call方法来执行接口方法的调用查询　
	 * 此API会将各个region上的信息放入Map
	 * <regionname,result>的结构中，搜集所有的结果后返回给调用者，调用者在循环进行归并计算。
	 * 
	 * @param config
	 * @param tableName
	 * @param startRowkey
	 * @param endRowkey
	 * @param type
	 * @param famillyName
	 * @param columnName
	 * @return
	 * @throws Throwable
	 */
	private static long multipleRegionsStatistics(Configuration config,
			String tableName, String startRowkey, String endRowkey,
			final String type, final String famillyName, final String columnName)
			throws Throwable {
		long result = 0;
		Table table = null;
		Connection connection = null;

		// 返回值接收，Map<region名称,计算结果>
		Map<byte[], getStatisticsResponse> results = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));

			// 第四个参数是接口类 Batch.Call。它定义了如何调用协处理器，用户通过重载该接口的 call() 方法来实现客户端的逻辑。在
			// call() 方法内，可以调用 RPC，并对返回值进行任意处理。
			Batch.Call<myStatisticsService, getStatisticsResponse> callable = new Batch.Call<myStatisticsService, getStatisticsResponse>() {
				ServerRpcController controller = new ServerRpcController();

				// 定义返回
				BlockingRpcCallback<getStatisticsResponse> rpcCallback = new BlockingRpcCallback<getStatisticsResponse>();

				// 下面重载 call 方法，API会连接到region后会运行call方法来执行服务的请求
				@Override
				public getStatisticsResponse call(myStatisticsService instance)
						throws IOException {
					// Server 端会进行慢速的遍历 region 的方法进行统计
					MyStatisticsInterface.getStatisticsRequest.Builder request = MyStatisticsInterface.getStatisticsRequest
							.newBuilder();
					request.setType(type);
					if (null != famillyName) {
						request.setFamillyName(famillyName);
					}

					if (null != columnName) {
						request.setColumnName(columnName);
					}
					// RPC 接口方法调用
					instance.getStatisticsResult(controller, request.build(),
							rpcCallback);
					// 直接返回结果，即该 Region 的 计算结果
					return rpcCallback.get();

				}
			};

			/**
			 * 通过Table.coprocessorService(Class, byte[], byte[],Batch.Call)
			 * 请求多region服务
			 * 
			 * 在这个API参数列表中，
			 * 　　　*　Class代表所需要请求的服务，当前是我们定义在proto中的myStatisticsService服务。
			 * 　　　*　byte[],
			 * byte[]参数指明了startRowKey和endRowKey，当都为null的时候即进行全表的全region的数据计算。
			 * 　　　*　Batch.Call：需要自定义，API会根据如上参数信息并行的连接各个region，
			 * 来执行这个参数中定义的call方法来执行接口方法的调用查询　
			 * 此API会将各个region上的信息放入Map<regionname,
			 * result>的结构中，搜集所有的结果后返回给调用者，调用者在循环进行归并计算。
			 */
			results = table.coprocessorService(myStatisticsService.class,
					Bytes.toBytes(startRowkey), Bytes.toBytes(endRowkey),
					callable);

			// 取得结果值后循环将结果合并，即得到最终的结果
			Collection<getStatisticsResponse> resultsc = results.values();
			for (getStatisticsResponse r : resultsc) {
				result += r.getResult();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != table) {
				table.close();
			}

			if (null != connection) {
				connection.close();
			}
		}
		return result;
	}

	/**
	 * 通过Table.coprocessorService(Class, byte[], byte[], Batch.Call,
	 * Batch.Callback) 请求多region服务
	 * 
	 * 在这个API参数列表中， 　　　*　Class代表所需要请求的服务，当前是我们定义在proto中的myStatisticsService服务。
	 * 　　　*　byte[],
	 * byte[]参数指明了startRowKey和endRowKey，当都为null的时候即进行全表的全region的数据计算。
	 * 　　　*　Batch.
	 * Call：需要自定义，API会根据如上参数信息并行的连接各个region，来执行这个参数中定义的call方法来执行接口方法的调用查询　
	 * 此API会将各个region上的信结果信息放入Map通过Callback定义进行处理，处理完所有所有的结果后返回给调用者。
	 * 
	 * @param config
	 * @param tableName
	 * @param startRowkey
	 * @param endRowkey
	 * @param type
	 * @param famillyName
	 * @param columnName
	 * @return
	 * @throws Throwable
	 */
	private static long multipleRegionsCallBackStatistics(Configuration config,
			String tableName, String startRowkey, String endRowkey,
			final String type, final String famillyName, final String columnName)
			throws Throwable {
		final AtomicLong atoResult = new AtomicLong();
		Table table = null;
		Connection connection = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));

			// 第四个参数是接口类 Batch.Call。它定义了如何调用协处理器，用户通过重载该接口的 call() 方法来实现客户端的逻辑。在
			// call() 方法内，可以调用 RPC，并对返回值进行任意处理。
			Batch.Call<myStatisticsService, getStatisticsResponse> callable = new Batch.Call<myStatisticsService, getStatisticsResponse>() {
				ServerRpcController controller = new ServerRpcController();

				// 定义返回
				BlockingRpcCallback<getStatisticsResponse> rpcCallback = new BlockingRpcCallback<getStatisticsResponse>();

				// 下面重载 call 方法，API会连接到region后会运行call方法来执行服务的请求
				@Override
				public getStatisticsResponse call(myStatisticsService instance)
						throws IOException {
					// Server 端会进行慢速的遍历 region 的方法进行统计
					MyStatisticsInterface.getStatisticsRequest.Builder request = MyStatisticsInterface.getStatisticsRequest
							.newBuilder();
					request.setType(type);
					if (null != famillyName) {
						request.setFamillyName(famillyName);
					}

					if (null != columnName) {
						request.setColumnName(columnName);
					}
					// RPC 接口方法调用
					instance.getStatisticsResult(controller, request.build(),
							rpcCallback);
					// 直接返回结果，即该 Region 的 计算结果
					return rpcCallback.get();

				}
			};

			// 定义 callback
			Batch.Callback<getStatisticsResponse> callback = new Batch.Callback<getStatisticsResponse>() {
				@Override
				public void update(byte[] region, byte[] row,
						getStatisticsResponse result) {
					// 直接将 Batch.Call 的结果，即单个 region 的 计算结果 累加到 atoResult
					atoResult.getAndAdd(result.getResult());
				}
			};

			/**
			 * 通过Table.coprocessorService(Class, byte[],
			 * byte[],Batch.Call,Batch.Callback) 请求多region服务
			 * 
			 * 在这个API参数列表中，
			 * 　　　*　Class代表所需要请求的服务，当前是我们定义在proto中的myStatisticsService服务。
			 * 　　　*　byte[],
			 * byte[]参数指明了startRowKey和endRowKey，当都为null的时候即进行全表的全region的数据计算。
			 * 　　　*　Batch.Call：需要自定义，API会根据如上参数信息并行的连接各个region，
			 * 来执行这个参数中定义的call方法来执行接口方法的调用查询　
			 * 此API会将各个region上的信结果信息放入Map通过Callback定义进行处理，处理完所有所有的结果后返回给调用者。
			 */
			table.coprocessorService(myStatisticsService.class,
					Bytes.toBytes(startRowkey), Bytes.toBytes(endRowkey),
					callable, callback);


		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != table) {
				table.close();
			}

			if (null != connection) {
				connection.close();
			}
		}
		return atoResult.longValue();
	}
}
