import dayjs from 'dayjs'
import * as log4js from 'log4js'
import { PubSubUtil } from './pubsub'

const logger = log4js.getLogger()
logger.level = 'all'

// 環境変数読み込み
require('dotenv').config()
const projectId = process.env.PROJECT || ''
const subscriptionName = process.env.SUBSCRIPTION
const topic = process.env.TOPIC || ''

// PubSub操作クラス
const pubsub = new PubSubUtil(projectId,subscriptionName)

// attr設定用カウンタ
let count = 1

// キー入力による分岐
process.stdin.setRawMode(true)
process.stdin.resume()
process.stdin.on('data',async (key)=>{
  let c = key.readUInt8()
  switch(c){
    case 0x31:  // 1
      // メッセージ送信
      {
        logger.info(`### publishMessage`)
        let msg = { timestamp: dayjs().toISOString() }
        let attr = { count: String(count++) }
        // 送信
        let msgId = await pubsub.publishMessage(topic,JSON.stringify(msg),attr).catch(reason=>{
          logger.error(reason)
          return undefined
        })
        logger.info(`msgId: ${msgId}`)
      }
      break
    case 0x32:  // 2
      // メッセージ受信
      {
        logger.info(`### pullMessages`)
        // 受信
        const msgs = await pubsub.pullMessages().catch(reason=>{
          logger.error(reason)
          return undefined
        })
        logger.info(JSON.stringify(msgs))
      }
      break
    case 0x1b:  // ESC
    case 0x03:  // ETX (CTRL+C)
      process.exit()
      break
    default:
      logger.info(`key: ${c.toString(16)}`)
      break
  }
})

