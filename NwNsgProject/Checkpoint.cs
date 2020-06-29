using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace nsgFunc
{
    public class Checkpoint : TableEntity
    {
        public int CheckpointIndex { get; set; }  // index of the last processed block list item

        public Checkpoint()
        {
        }

        public Checkpoint(string partitionKey, string rowKey, string blockName, long offset, int index)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            CheckpointIndex = index;
        }

        public static Checkpoint GetCheckpoint(BlobDetails blobDetails, CloudTable checkpointTable)
        {
            try 
            {
            TableOperation operation = TableOperation.Retrieve<Checkpoint>(
                blobDetails.GetPartitionKey(), blobDetails.GetRowKey());
                }
                catch (Exception e)
                {
                Console.WriteLine("{0} 1 error", e);
                }
            try
            {
            TableResult result = checkpointTable.ExecuteAsync(operation).Result;
            }
            catch (Exception e)
            {
            Console.WriteLine("{0} 2 error", e);
            }
            
            try {
            Checkpoint checkpoint = (Checkpoint)result.Result; }
            catch (Exception e)
            {
            Console.WriteLine("{0} 3 error", e);
            }
            if (checkpoint == null)
            {
                try {
                checkpoint = new Checkpoint(blobDetails.GetPartitionKey(), blobDetails.GetRowKey(), "", 0, 1);}
                catch (Exception e)
                {
                Console.WriteLine("{0} 4 error", e);
                }
            }
            if (checkpoint.CheckpointIndex == 0)
            {
                try {
                checkpoint.CheckpointIndex = 1;}
                catch (Exception e)
                {
                Console.WriteLine("{0} 5 error", e);
                }
            }

            return checkpoint;
        }

        public void PutCheckpoint(CloudTable checkpointTable, int index)
        {
            CheckpointIndex = index;
            
            try {
            TableOperation operation = TableOperation.InsertOrReplace(this);}
            catch (Exception e)
            {
            Console.WriteLine("{0} 6 error", e);
            }
            try {
            checkpointTable.ExecuteAsync(operation).Wait();}
            catch (Exception e)
            {
            Console.WriteLine("{0} 7 error", e);
            }
        }
    }
}
