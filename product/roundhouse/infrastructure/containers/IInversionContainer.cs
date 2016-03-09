namespace roundhouse.infrastructure.containers
{
    public interface IInversionContainer
    {
        TypeToReturn Resolve<TypeToReturn>();
    }
}