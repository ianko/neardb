import NearDB from '../neardb'
import { PathList, BaseEntity, Payload } from '../types'
import { reservedKey, uuid, documentPath, collectionIndicesPath } from './utils'
import Document from './document'

export default class Collection implements BaseEntity {
  readonly path: PathList
  readonly dbPath: string
  readonly indicesPath: string
  readonly instance: NearDB

  constructor(instance: NearDB, key: string, path: PathList) {
    // Check if this is a reserved keyword
    if (reservedKey(key)) {
      throw new Error(key + ': is a reserved keyword')
    }

    this.instance = instance

    // Copy value of path before passing, to avoid poluting scope
    let newPath = [...path]

    newPath.push({
      type: 'collection',
      key
    })

    this.path = newPath
    this.dbPath = documentPath(this.path)
    this.indicesPath = collectionIndicesPath(this.path)
  }

  doc(key: string) {
    return new Document(this.instance, key, this.path)
  }

  /**
   * Adds a document to a collection by generating an id for the doc
   * @param value expects payload to be stored for the document
   * @returns a promise for the payload of the saved doc
   */
  add(value: Payload): Promise<object> {
    return new Document(this.instance, uuid(), this.path).set(value)
  }

  private async checkLockFile() {
    try {
      return await this.instance.adapter.head(this.indicesPath)
    } catch (err) {
      // TODO: Check only if key does not
      return this.instance.adapter.set({}, this.indicesPath)
    }
  }

  private async lockCollection() {
    let isOwner: boolean
    let lockExpired: boolean
    let lockFile: any

    try {
      await this.checkLockFile()
      lockFile = await this.instance.adapter.copy(
        this.indicesPath,
        lockFile.ETag,
        {
          instance: this.instance.instanceId
        }
      )
      lockExpired = lockFile.LastModified + 3000 < new Date().getTime()
      isOwner = lockFile.Metadata.instance === this.instance.instanceId

      // Lock still valid
      if (isOwner && !lockExpired) return true

      // TODO: Better error message depending on the issue
      throw new Error('Cannot lock collection')
    } catch (err) {
      throw err
    }
  }

  /**
   * Updates indices of collection from a new document
   * @param documentKey Key of the object to update the collection json file
   * @param value value that needs to be added to indices
   * @returns a promise for the adapter put
   */
  async updateCollectionIndices(
    documentKey: string,
    value: Payload
  ): Promise<Payload> {
    let collectionIndices: any

    try {
      // Get current collection indices
      collectionIndices = await this.instance.adapter.get(this.indicesPath)
      // Use document key as key in the object, and store value
      collectionIndices[documentKey] = value
      // Save object into collection indices document
      return await this.instance.adapter.set(
        collectionIndices,
        this.indicesPath
      )
    } catch (err) {
      throw err
    }
  }
}
