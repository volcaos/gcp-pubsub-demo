import dayjs from 'dayjs'
import { Attributes, PubSub, v1 } from '@google-cloud/pubsub'
import { google } from '@google-cloud/pubsub/build/protos/protos'

/**
 * Wrapper class for PubSub
 * 
 */
export class PubSubUtil
{
  /**
   * PubSub本体
   */
  pubSub : PubSub
  /**
   * Pull用クライアント
   */
  subscriberClient : v1.SubscriberClient
  /**
   * subscriptionPath生成用のプロジェクトID
   */
  projectId : string
  /**
   * subscriptionPath生成用のSubscription名（最初に設定しておくと後に省略できる）
   */
  defaultSubscriptionName : string | undefined
  /**
   * Pull時の最大メッセージ数
   */
  maxMessages : number
  // 
  /**
   * constructor
   * 
   * @param {string} [projectId]
   * @param {string} [defaultSubscriptionName]
   * @param {string} [maxMessages]
   */
  constructor( projectId:string, defaultSubscriptionName?:string, maxMessages?:number ) {
    this.pubSub = new PubSub()
    this.subscriberClient = new v1.SubscriberClient()
    // 
    this.projectId = projectId
    this.defaultSubscriptionName = defaultSubscriptionName
    this.maxMessages = maxMessages || 100
  }
  /**
   * メッセージ送信
   * 
   * Publishing messages to topics (https://cloud.google.com/pubsub/docs/publisher)
   * @param topic トピック名
   * @param message メッセージ本文
   * @param attributes 付加情報
   * @returns Message ID
   */
  publishMessage = async (topic:string,message:string,attributes?:Attributes) => {
    let opt = {
      batching: undefined,
      gaxOpts: undefined,
      messageOrdering: undefined,
      enableOpenTelemetryTracing: undefined,
    }
    let msgId = await this.pubSub.topic(topic,opt).publish(Buffer.from(message),attributes)
    return msgId
  }
  /**
   * メッセージ受信
   * 
   * Asynchronous Pull (https://cloud.google.com/pubsub/docs/pull)
   * @param subscriptionName 
   * @returns Array of Message
   */
  pullMessages = async (subscriptionName?:string) => {
    // subscriptionNameが省略されていたらdefaultSubscriptionNameを使う
    const subscription = subscriptionName || this.defaultSubscriptionName
    if( !subscription ){
      throw new Error(`subscriptionName is empty.`)
    }
    // subscriptionPath生成
    const subscriptionPath = this.subscriberClient.subscriptionPath(this.projectId,subscription)
    // リクエスト生成
    const req = {
      subscription: subscriptionPath,
      maxMessages: this.maxMessages,
      returnImmediately: false,
    }
    // 受信
    const [res] = await this.subscriberClient.pull(req)
    // ackId抽出
    const ackIds = res.receivedMessages?.map((msg)=>msg.ackId||'').filter(id=>id) || []
    // ack送信
    if( ackIds.length > 0 ){
      const ackReq = {
        subscription: subscriptionPath,
        ackIds: ackIds
      }
      await this.subscriberClient.acknowledge(ackReq)
    }
    // 
    return res.receivedMessages || []
  }
  /**
   * TimestampをISO8061文字列に変換
   * 
   * @param ts  Timestamp of message
   * @returns ISO8061 string
   */
  parsePublishTime = (ts?:google.protobuf.ITimestamp) => {
    return ts ? dayjs(Number(ts?.seconds)*1000+Number(ts?.nanos)/1e6).toISOString() : undefined
  }
}
