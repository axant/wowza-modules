package it.axant.todostreaming.wowza;

import java.util.ArrayList;
import java.util.List;

import com.wowza.wms.amf.AMFData;
import com.wowza.wms.amf.AMFDataList;
import com.wowza.wms.amf.AMFDataMixedArray;
import com.wowza.wms.amf.AMFDataObj;
import com.wowza.wms.amf.AMFPacket;
import com.wowza.wms.application.WMSProperties;
import com.wowza.wms.client.IClient;
import com.wowza.wms.module.ModuleBase;
import com.wowza.wms.request.RequestFunction;
import com.wowza.wms.stream.IMediaStream;
import com.wowza.wms.stream.IMediaStreamActionNotify2;
import com.wowza.wms.stream.IMediaStreamMetaDataProvider;

public class MetaDataListener extends ModuleBase {
	private AMFDataMixedArray amfDataMixedArray;
	private AMFDataList amfDataList;
	
	public void onStreamCreate(IMediaStream stream) {
		getLogger().info("onStreamCreate by: " + stream.getClientId());
		IMediaStreamActionNotify2 actionNotify = new StreamListener();

		WMSProperties props = (WMSProperties) stream.getProperties();
		synchronized (props) {
			props.put("streamActionNotifier", actionNotify);
		}
		stream.addClientListener(actionNotify);
		// sendResult(client, params, data);
	}

	public void onStreamDestroy(IMediaStream stream) {
		getLogger().info("onStreamDestroy by: " + stream.getClientId());

		IMediaStreamActionNotify2 actionNotify = null;
		WMSProperties props = stream.getProperties();
		synchronized (props) {
			actionNotify = (IMediaStreamActionNotify2) stream.getProperties().get("streamActionNotifier");
		}
		if (actionNotify != null) {
			stream.removeClientListener(actionNotify);
			getLogger().info("removeClientListener: " + stream.getSrc());
		}
	}

	public void setAmfDataMixedArray(AMFDataMixedArray amfDataMixedArray) {
		this.amfDataMixedArray = amfDataMixedArray;
	}

	public AMFDataMixedArray getAmfDataMixedArray() {
		return amfDataMixedArray;
	}

	public void onConnect(IClient client, RequestFunction function, AMFDataList params)
    {
		setAmfDataList(params);		
    }
	
	public void setAmfDataList(AMFDataList amfDataList) {
		this.amfDataList = amfDataList;
	}

	public AMFDataList getAmfDataList() {
		return amfDataList;
	}

	class StreamListener implements IMediaStreamActionNotify2 {

		public void onMetaData(IMediaStream stream, AMFPacket metaDataPacket) {
			IMediaStreamMetaDataProvider metaDataProvider = stream.getMetaDataProvider();

			while (true) {
				if (metaDataProvider == null)
					break;

				List<AMFPacket> metaDataList = new ArrayList<AMFPacket>();
				long firstTimecode = 0;
				metaDataProvider.onStreamStart(metaDataList, firstTimecode);

				if (metaDataList.size() <= 0)
					break;

				AMFPacket metaPacket = (AMFPacket) metaDataList.get(0);
				AMFDataList dataList = new AMFDataList(metaPacket.getData());

				if (dataList.size() < 2)
					break;

				if (dataList.get(1).getType() == AMFData.DATA_TYPE_OBJECT) {
					AMFDataObj obj = (AMFDataObj) dataList.get(1);
					AMFDataMixedArray arr = new AMFDataMixedArray();
					
					for (int i=0; i<obj.size(); i++) {
						arr.put(obj.getKey(i), obj.get(i));
					}
					
					dataList.set(1, arr);
					setAmfDataMixedArray(arr);
					
				/*
					dataList.set(0, (AMFDataMixedArray) obj);
					
					metaDataProvider.onStreamStart(metaDataList, firstTimecode);
					
					getLogger().error(arr.toString());
					sendResult(stream.getClient(), dataList, arr);
				 */
				}

				break;
			}
		}
		
		@Override
		public void onPause(IMediaStream stream, boolean isPause,
				double location) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onPlay(IMediaStream stream, String streamName,
				double playStart, double playLen, int playReset) {
			getLogger().info("==================>  onPlay " + stream.getClientId());
			getLogger().info("==================>  mixeArray " + getAmfDataMixedArray());
			getLogger().info("==================>  dataList " + getAmfDataList());
			sendResult(stream.getClient(), getAmfDataList(), getAmfDataMixedArray());
		}

		@Override
		public void onPublish(IMediaStream stream, String streamName,
				boolean isRecord, boolean isAppend) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onSeek(IMediaStream stream, double location) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onStop(IMediaStream stream) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onUnPublish(IMediaStream stream, String streamName,
				boolean isRecord, boolean isAppend) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onPauseRaw(IMediaStream stream, boolean isPause,
				double location) {
			// TODO Auto-generated method stub

		}

	}

}