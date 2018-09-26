using System.Collections.Generic;

namespace KafkaSandbox
{
    public class EventTypes
    {
        public const string FolderAssignedEvent = "FolderAssignedEvent";

        public const string GroupAssignedEvent = "GroupAssignedEvent";

        public static IEnumerable<string> AllEventTypes
        { 
            get 
            {
                return new List<string>
                {
                    FolderAssignedEvent,
                    GroupAssignedEvent
                };
            }
        }
    }
}