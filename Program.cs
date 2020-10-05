using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDBTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var CONNECTION_STRING = "YOUR_CONNECTION_STRING";
                var DB_NAME = "Customer";
                var dbService = new MongoDBService(CONNECTION_STRING, DB_NAME);
                while (true)
                {

                    Console.WriteLine("Please input step need to run: ");
                    Console.WriteLine("0 - Insert ");
                    Console.WriteLine("1 - Update ");
                    Console.WriteLine("2 - FindOne ");
                    Console.WriteLine("3 - FindMany ");
                    Console.WriteLine("4 - Delete ");
                    var work_step = Console.ReadLine();

                    // CODE FOR INSERT MANY DOCUMENTS TO COLLECTION
                    //for (int i = 1; i <= 1000000; i++)
                    //{
                    //    var document = new BsonDocument {
                    //    {"CifNo", $"CifNo{i}" },
                    //    {"Name",  $"Name{i}" },
                    //    {"Accounts",  new BsonArray {
                    //                        new BsonDocument {
                    //                            { "AccountNo", $"AccountNo{i}_1" },
                    //                            { "Emails", new BsonArray { $"Email{i}_1_1", $"Email{i}_1_2", $"Email{i}_1_3" } },
                    //                            { "Devices", new BsonArray { $"Device{i}_1_1", $"Device{i}_1_2", $"Device{i}_1_3" } },
                    //                            { "Mobiles", new BsonArray { "Mobile1_1_1", "Mobile1_1_2", "Mobile1_1_3" } }
                    //                        },
                    //                        new BsonDocument {
                    //                            { "AccountNo", $"AccountNo{i}_2" },
                    //                            { "Emails", new BsonArray { $"Email{i}_2_1", $"Email{i}_2_2", $"Email{i}_2_3" } },
                    //                            { "Devices", new BsonArray { $"Device{i}_2_1", $"Device{i}_2_2", $"Device{i}_2_3" } },
                    //                            { "Mobiles", new BsonArray { $"Mobile{i}_2_1", $"Mobile{i}_2_2", $"Mobile{i}_2_3" } }
                    //                        },
                    //                        new BsonDocument {
                    //                            { "AccountNo", $"AccountNo{i}_3" },
                    //                            { "Emails", new BsonArray { $"Email{i}_3_1", $"Email{i}_3_2", $"Email{i}_3_3" } },
                    //                            { "Devices", new BsonArray { $"Device{i}_3_1", $"Device{i}_3_2", $"Device{i}_3_3" } },
                    //                            { "Mobiles", new BsonArray { $"Mobile{i}_3_1", $"Mobile{i}_3_2", $"Mobile{i}_3_3" } }
                    //                            }
                    //                    }},
                    //        { "IsSMS", i % 2 == 0 ? true : false },
                    //        { "IsNotiMB",  i % 5 == 0 ? true : false },
                    //        { "IsNotiSP",  i % 7 == 0 ? true : false }
                    //    };
                    //    collection.InsertOne(document);
                    //    Console.WriteLine($"Insert document -- {i} -- complete!");
                    //    Thread.Sleep(5);
                    //}


                    var i = 1;
                    switch (work_step)
                    {
                        default: //Insert
                            Console.WriteLine("==Insert==");
                            Console.WriteLine("Please insert index: ");
                            i = Int32.Parse(Console.ReadLine());
                            var document = new BsonDocument {
                            {"CifNo", $"CifNo{i}" },
                            {"Name",  $"Name{i}" },
                            {"Accounts",  new BsonArray {
                                                new BsonDocument {
                                                    { "AccountNo", $"AccountNo{i}_1" },
                                                    { "Emails", new BsonArray { $"Email{i}_1_1", $"Email{i}_1_2", $"Email{i}_1_3" } },
                                                    { "Devices", new BsonArray { $"Device{i}_1_1", $"Device{i}_1_2", $"Device{i}_1_3" } },
                                                    { "Mobiles", new BsonArray { "Mobile1_1_1", "Mobile1_1_2", "Mobile1_1_3" } }
                                                },
                                                new BsonDocument {
                                                    { "AccountNo", $"AccountNo{i}_2" },
                                                    { "Emails", new BsonArray { $"Email{i}_2_1", $"Email{i}_2_2", $"Email{i}_2_3" } },
                                                    { "Devices", new BsonArray { $"Device{i}_2_1", $"Device{i}_2_2", $"Device{i}_2_3" } },
                                                    { "Mobiles", new BsonArray { $"Mobile{i}_2_1", $"Mobile{i}_2_2", $"Mobile{i}_2_3" } }
                                                },
                                                new BsonDocument {
                                                    { "AccountNo", $"AccountNo{i}_3" },
                                                    { "Emails", new BsonArray { $"Email{i}_3_1", $"Email{i}_3_2", $"Email{i}_3_3" } },
                                                    { "Devices", new BsonArray { $"Device{i}_3_1", $"Device{i}_3_2", $"Device{i}_3_3" } },
                                                    { "Mobiles", new BsonArray { $"Mobile{i}_3_1", $"Mobile{i}_3_2", $"Mobile{i}_3_3" } }
                                                    }
                                            }},
                                { "IsSMS", i % 2 == 0 ? true : false },
                                { "IsNotiMB",  i % 5 == 0 ? true : false },
                                { "IsNotiSP",  i % 7 == 0 ? true : false }
                            };


                            var result = await dbService.InsertOneAsync("CustomerProfiles", document);


                            Console.WriteLine($"Result: {result}");
                            break;
                        case "1":
                            Console.WriteLine("==Update==");
                            Console.WriteLine("Please insert index, keyUpdate, valueUpdate: ");
                            i = Int32.Parse(Console.ReadLine());
                            var keyUpdate = Console.ReadLine();
                            var valueUpdate = Console.ReadLine();
                            var update_field = Builders<BsonDocument>.Update.Set(keyUpdate, valueUpdate);

                            var result_update = await dbService.UpdateOneAsync("CustomerProfiles", "Accounts.AccountNo", $"AccountNo{i}_3", update_field);

                            
                            Console.WriteLine($"Result: {result_update}");
                            break;
                        case "2":
                            Console.WriteLine("==FindOne==");
                            Console.WriteLine("Please insert index: ");
                            i = Int32.Parse(Console.ReadLine());

                            BsonDocument result_find_one = await dbService.SelectOneAsync<BsonDocument>("CustomerProfiles", "CifNo", $"CifNo{i}");
                            Console.WriteLine(result_find_one);
                            break;
                        case "3":
                            Console.WriteLine("==FindMany==");

                            List<BsonDocument> result_find_many = await dbService.SelectAsync<BsonDocument, bool>("CustomerProfiles", "IsSMS", true, 10);
                            Console.WriteLine(result_find_many.ToJson());
                            break;
                        case "4":
                            Console.WriteLine("==Delete==");
                            Console.WriteLine("Please insert index: ");
                            i = Int32.Parse(Console.ReadLine());

                            var check = await dbService.DeleteOneAsync("CustomerProfiles", "CifNo", $"CifNo{i}");
                            Console.WriteLine($"Result remove CifNo{i}: {check}");
                            break;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex);
            }

            Console.ReadKey();
        }
    }
}
